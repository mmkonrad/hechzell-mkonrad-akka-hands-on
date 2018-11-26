package de.hpi.octopus.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.metrics.AdaptiveLoadBalancingPool;
import akka.cluster.metrics.SystemLoadAverageMetricsSelector;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import akka.util.Timeout;
import de.hpi.octopus.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import scala.Function1;
import scala.Int;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import de.hpi.octopus.messages.ShutdownMessage;

public class Master extends AbstractActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private ActorRef sender;

    public static Props props() {
        return Props.create(Master.class);
    }

    Router workerRouter = new Router(new RoundRobinRoutingLogic());

    Map<String, Integer> crackedPasswords = new HashMap<String, Integer>();
    Map<String, String> sequences = new HashMap<String, String>();
    Map<String, String> hashes = new HashMap<String, String>();
    boolean solvedPrefixes = false;

    ////////////////////
    // Actor messages //
    ////////////////////

    @Data
    @AllArgsConstructor
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 4545299661052078209L;
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class TaskMessage implements Serializable {
        private static final long serialVersionUID = -8330958742629706627L;

        private TaskMessage() {
        }

        private int attributes;
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class CompletionMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;

        public enum status {MINIMAL, EXTENDABLE, FALSE, FAILED}

        private CompletionMessage() {
        }

        private status result;
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class SecretsTaskMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, String> Map = new HashMap<String, String>(42);
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class SecretRevealedMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, Integer> Map = new HashMap<String, Integer>();

        public SecretRevealedMessage() {
        }
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class SequenceTaskMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, String> Map = new HashMap<String, String>(42);
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class SequenceRevealedMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, String> Map = new HashMap<String, String>();

        public SequenceRevealedMessage() {
        }
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class LinearTaskMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, Integer> Map = new HashMap<String, Integer>(42);
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class LinearRevealedMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, Integer> Map = new HashMap<String, Integer>();

        public LinearRevealedMessage() {
        }
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class HashTaskMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, String> Seq = new HashMap<String, String>(42);
        private Map<String, Integer> Lin = new HashMap<String, Integer>(42);
    }

    @Data
    @AllArgsConstructor
    @SuppressWarnings("unused")
    public static class HashRevealedMessage implements Serializable {
        private static final long serialVersionUID = -6823011111281387872L;
        private Map<String, String> Map = new HashMap<String, String>();

        public HashRevealedMessage() {
        }
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
                .match(SequenceTaskMessage.class, this::handle)
                .match(SequenceRevealedMessage.class, this::handle)
                .match(LinearTaskMessage.class, this::handle)
                .match(LinearRevealedMessage.class, this::handle)
                .match(HashTaskMessage.class, this::handle)
                .match(HashRevealedMessage.class, this::handle)
                .match(ShutdownMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(SecretsTaskMessage message) throws InterruptedException {
        this.sender = getSender();
        this.crackedPasswords = new HashMap<String, Integer>();
        final int chunkSize = 1000000 / this.workerRouter.routees().size();
        Map<String, String> hashes = message.Map;

        for (int i = 0; i < this.idleWorkers.size(); i++) {
            int currentStartNumber = (i * chunkSize);
            int currentEndNumber = currentStartNumber + chunkSize - 1;
            // Handle any remainder if this is the last worker
            if (i == this.idleWorkers.size() - 1)
                currentEndNumber = 1000000;
//            System.out.println("start: " + currentStartNumber + " end: " + currentEndNumber);
            this.workerRouter.route(new Worker.SecretsSubTaskMessage(hashes, currentStartNumber, currentEndNumber), this.self());
        }
    }


    private void handle(SecretRevealedMessage message) {
//        System.out.println("id: " + message.Map.keySet() + " cleartext: " + message.Map.values());
        Map.Entry<String, Integer> entry = message.Map.entrySet().iterator().next();
        this.crackedPasswords.put(entry.getKey(), entry.getValue());
//        System.out.println(this.crackedPasswords.size());
        if (this.crackedPasswords.size() >= 42) {
            this.sender.tell(this.crackedPasswords, this.sender);
        }
    }

    private void handle(SequenceTaskMessage message) throws InterruptedException {
        this.sender = getSender();
        final int chunkSize = 42 / this.workerRouter.routees().size();
        Map<String, String> sequences = message.Map;

        for (int i = 0; i < this.idleWorkers.size(); i++) {
            int currentStartNumber = (i * chunkSize) + 1;
            int currentEndNumber = currentStartNumber + chunkSize - 1;
            // Handle any remainder if this is the last worker
            if (i == this.idleWorkers.size() - 1)
                currentEndNumber = 42;
//            System.out.println("start: " + currentStartNumber + " end: " + currentEndNumber);
            this.workerRouter.route(new Worker.SequenceSubTaskMessage(sequences, currentStartNumber, currentEndNumber), this.self());
        }
    }

    private void handle(SequenceRevealedMessage message) {
//        System.out.println("id: " + message.Map.keySet() + " seq: " + message.Map.values());
        Map.Entry<String, String> entry = message.Map.entrySet().iterator().next();
        this.sequences.put(entry.getKey(), entry.getValue());
//        System.out.println(this.sequences.size());
        if (this.sequences.size() >= 42) {
            this.sender.tell(this.sequences, this.sender);
        }
    }

    private void handle(LinearTaskMessage message) throws InterruptedException {
        this.sender = getSender();
        long maxNumber = (long) Math.pow(2, 43);
        final long chunkSize = maxNumber / this.workerRouter.routees().size();
        Map<String, Integer> passwords = message.Map;

        for (int i = 0; i < this.idleWorkers.size(); i++) {
            long currentStartNumber = (i * chunkSize);
            long currentEndNumber = currentStartNumber + chunkSize - 1;
            // Handle any remainder if this is the last worker
            if (i == this.idleWorkers.size() - 1)
                currentEndNumber = maxNumber;
//            System.out.println("start: " + currentStartNumber + " end: " + currentEndNumber);
            this.workerRouter.route(new Worker.LinearSubTaskMessage(passwords, currentStartNumber, currentEndNumber), this.self());
        }
    }

    private void handle(LinearRevealedMessage message) {
//        System.out.println(message.Map);
        if (!this.solvedPrefixes) {
            this.solvedPrefixes = true;

            for (ActorRef worker: this.idleWorkers){
                worker.tell(new Worker.AbortMessage(), this.self());
            }

            this.sender.tell(message.Map, this.sender);
        }
    }

    private void handle(HashTaskMessage message) throws InterruptedException {
        System.out.println("Start hash generation");
        this.sender = getSender();
        final int chunkSize = 42 / this.workerRouter.routees().size();
        Map<String, String> partners = message.Seq;
        Map<String, Integer> prefixes = message.Lin;

        for (int i = 0; i < this.idleWorkers.size(); i++) {
            int currentStartNumber = (i * chunkSize) + 1;
            int currentEndNumber = currentStartNumber + chunkSize - 1;
            // Handle any remainder if this is the last worker
            if (i == this.idleWorkers.size() - 1)
                currentEndNumber = 42;
//            System.out.println("start: " + currentStartNumber + " end: " + currentEndNumber);
            this.workerRouter.route(new Worker.HashSubTaskMessage(partners, prefixes, currentStartNumber, currentEndNumber), this.self());
        }
    }

    private void handle(HashRevealedMessage message) {
//        System.out.println("id: " + message.Map.keySet() + " seq: " + message.Map.values());
        Map.Entry<String, String> entry = message.Map.entrySet().iterator().next();
        this.hashes.put(entry.getKey(), entry.getValue());
//        System.out.println(this.sequences.size());
        if (this.hashes.size() >= 42) {
            this.sender.tell(this.hashes, this.sender);
        }
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        // Register at this actor system's reaper
        Reaper.watchWithDefaultReaper(this);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
    }


    private void handle(ShutdownMessage message) {
        for (ActorRef worker : idleWorkers) {
            System.out.println("Sending Shutdown to worker: " + worker.toString());
            worker.tell(new ShutdownMessage(), ActorRef.noSender());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Stop self and all child actors by sending a poison pill
        this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
    }


    private void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.idleWorkers.add(this.sender());


        //this.assign(this.sender());
        // add worker to router
        this.workerRouter = this.workerRouter.addRoutee(this.sender());

        System.out.println("#Registered Workers:" + this.idleWorkers.size());
        System.out.println("#Routable Workers:" + this.workerRouter.routees().size());


        this.log.info("Registered {}", this.sender());
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.getActor());
    }
}