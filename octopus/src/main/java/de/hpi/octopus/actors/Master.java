package de.hpi.octopus.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.cluster.Cluster;
import akka.cluster.metrics.AdaptiveLoadBalancingPool;
import akka.cluster.metrics.SystemLoadAverageMetricsSelector;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Router;
import akka.util.Timeout;
import de.hpi.octopus.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import scala.Int;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Master extends AbstractActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private ActorRef sender;

    public static Props props() {
        return Props.create(Master.class);
    }

    /**
    int maxWorkersPerNode = 4;
    int maxWorkersPerCluster = 1000;
    boolean allowLocalWorkers = true;

    Set<String> roles = new HashSet<>(Arrays.asList("slave"));

    ActorRef router = this.getContext().system().actorOf(
        new ClusterRouterPool(
                new AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector.getInstance(), 0),
                new ClusterRouterPoolSettings(maxWorkersPerCluster, maxWorkersPerNode, allowLocalWorkers, roles)
        ).props(Props.create(Worker.class)), "router");
    **/

    Router workerRouter = new Router(new RoundRobinRoutingLogic());

    Map<String, Integer> crackedPasswords = new HashMap<String, Integer>();

    ////////////////////
    // Actor messages //
    ////////////////////

    @Data @AllArgsConstructor
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 4545299661052078209L;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class TaskMessage implements Serializable {
        private static final long serialVersionUID = -8330958742629706627L;
        private TaskMessage() {}
        private int attributes;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class CompletionMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        public enum status {MINIMAL, EXTENDABLE, FALSE, FAILED}
        private CompletionMessage() {}
        private status result;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class SecretsTaskMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, String> Map = new HashMap<String, String>(42);
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class SecretRevealedMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, Integer> Map = new HashMap<String, Integer>();
    }

    /////////////////
    // Actor State //
    /////////////////

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

    private TaskMessage task;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegistrationMessage.class, this::handle)
                .match(SecretsTaskMessage.class, this::handle)
                .match(SecretRevealedMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(TaskMessage.class, this::handle)
                .match(CompletionMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(SecretsTaskMessage message) throws InterruptedException {
        this.sender = getSender();
        final int chunkSize = 1000000 / this.workerRouter.routees().size();
        Map<String, String> hashes = message.Map;

        for (int i = 0; i < this.idleWorkers.size(); i++) {
            int currentStartNumber = (i * chunkSize);
            int currentEndNumber = currentStartNumber + chunkSize - 1;
            // Handle any remainder if this is the last worker
            if (i == this.idleWorkers.size() - 1)
                currentEndNumber = 1000000;
            System.out.println("start: " + currentStartNumber + " end: " + currentEndNumber);
            this.workerRouter.route(new Worker.SecretsSubTaskMessage(hashes, currentStartNumber, currentEndNumber), this.self());
        }
    }


    private void handle(SecretRevealedMessage message) {
//        System.out.println("id: " + message.Map.keySet() + " cleartext: " + message.Map.values());
        Map.Entry<String,Integer> entry = message.Map.entrySet().iterator().next();
        this.crackedPasswords.put(entry.getKey(), entry.getValue());
        if(this.crackedPasswords.size() >= 42){
            this.sender.tell(this.crackedPasswords, this.sender);
        }
    }










    private void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.assign(this.sender());

        // add worker to router
        this.workerRouter = this.workerRouter.addRoutee(this.sender());

        this.log.info("Registered {}", this.sender());
        //System.out.println(this.workerRouter.routees().size());
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.getActor());

        if (!this.idleWorkers.remove(message.getActor())) {
            WorkMessage work = this.busyWorkers.remove(message.getActor());
            if (work != null) {
                this.assign(work);
            }
        }
        this.log.info("Unregistered {}", message.getActor());
    }

    private void handle(TaskMessage message) {
        if (this.task != null)
            this.log.error("The master actor can process only one task in its current implementation!");

        this.task = message;
        this.assign(new WorkMessage(new int[0], new int[0]));
    }

    private void handle(CompletionMessage message) {
        ActorRef worker = this.sender();
        WorkMessage work = this.busyWorkers.remove(worker);

        this.log.info("Completed: [{},{}]", Arrays.toString(work.getX()), Arrays.toString(work.getY()));

        switch (message.getResult()) {
            case MINIMAL:
                this.report(work);
                break;
            case EXTENDABLE:
                this.split(work);
                break;
            case FALSE:
                // Ignore
                break;
            case FAILED:
                this.assign(work);
                break;
        }

        this.assign(worker);
    }

    private void assign(WorkMessage work) {
        ActorRef worker = this.idleWorkers.poll();

        if (worker == null) {
            this.unassignedWork.add(work);
            return;
        }

        this.busyWorkers.put(worker, work);
        worker.tell(work, this.self());
    }

    private void assign(ActorRef worker) {
        WorkMessage work = this.unassignedWork.poll();

        if (work == null) {
            this.idleWorkers.add(worker);
            return;
        }

        this.busyWorkers.put(worker, work);
        worker.tell(work, this.self());
    }

    private void report(WorkMessage work) {
        this.log.info("UCC: {}", Arrays.toString(work.getX()));
    }

    private void split(WorkMessage work) {
        int[] x = work.getX();
        int[] y = work.getY();

        int next = x.length + y.length;

        if (next < this.task.getAttributes() - 1) {
            int[] xNew = Arrays.copyOf(x, x.length + 1);
            xNew[x.length] = next;
            this.assign(new WorkMessage(xNew, y));

            int[] yNew = Arrays.copyOf(y, y.length + 1);
            yNew[y.length] = next;
            this.assign(new WorkMessage(x, yNew));
        }
    }
}