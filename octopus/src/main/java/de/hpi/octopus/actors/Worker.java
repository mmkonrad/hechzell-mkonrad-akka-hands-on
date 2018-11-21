package de.hpi.octopus.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Master.CompletionMessage;
import de.hpi.octopus.actors.Master.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class Worker extends AbstractActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    ////////////////////
    // Actor messages //
    ////////////////////

    /**
     * Asks the {@link Worker} to discover all primes in a given range.
     */
    public static class SecretsSubTaskMessage implements Serializable {

        private static final long serialVersionUID = -7467053227355130231L;
        private Map<String, String> hashes;
        private int start, end;

        public SecretsSubTaskMessage(Map<String, String> hashes, int start, int end) {
            this.hashes = hashes;
            this.start = start;
            this.end = end;
        }
        /**
         * For serialization/deserialization only.
         */
        @SuppressWarnings("unused")
        private SecretsSubTaskMessage() {
        }
    }


    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class WorkMessage implements Serializable {
        private static final long serialVersionUID = -7643194361868862395L;
        private WorkMessage() {}
        private int[] x;
        private int[] y;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
    private final Cluster cluster = Cluster.get(this.context().system());

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        this.cluster.subscribe(this.self(), MemberUp.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(SecretsSubTaskMessage.class, this::handle)
                .match(WorkMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }


    private void handle(SecretsSubTaskMessage message) {
        int start = message.start;
        int end = message.end;

        System.out.println("My Range: " + start + "-" + end);

        Map<String, String> hashes = message.hashes;
        Map<String, Integer> cleartext;

        for (int i = start; i <= end; i++) {
            String hash = this.hash(i);
            //System.out.println("Number: " + i + " Hash: " + hash);


            for (Map.Entry<String,String> entry : hashes.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

//                System.out.println("key: " + key + " value: " + value + " i: " + i + " hash: " + hash);

                /**
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                **/

                if (hash.equals(value)) {
                    System.out.println("Match!");
                    cleartext = new HashMap<String, Integer>();
                    cleartext.put(key, i);
                    this.sender().tell(new Master.SecretRevealedMessage(cleartext), this.self());
                }

            }
        }
    }








    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if (member.hasRole(OctopusMaster.MASTER_ROLE))
            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new RegistrationMessage(), this.self());
    }

    private void handle(WorkMessage message) {
        long y = 0;
        for (int i = 0; i < 1000000; i++)
            if (this.isPrime(i))
                y = y + i;

        this.log.info("done: " + y);

        this.sender().tell(new CompletionMessage(CompletionMessage.status.EXTENDABLE), this.self());
    }

    private boolean isPrime(long n) {

        // Check for the most basic primes
        if (n == 1 || n == 2 || n == 3)
            return true;

        // Check if n is an even number
        if (n % 2 == 0)
            return false;

        // Check the odds
        for (long i = 3; i * i <= n; i += 2)
            if (n % i == 0)
                return false;

        return true;
    }





    private String hash(int number) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(number).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        }
        catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }












}