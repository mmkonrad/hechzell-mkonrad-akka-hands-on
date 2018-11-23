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

    public static class SequenceSubTaskMessage implements Serializable {

        private static final long serialVersionUID = -7467053227355130231L;
        private Map<String, String> sequences;
        private int start, end;

        public SequenceSubTaskMessage(Map<String, String> sequences, int start, int end) {
            this.sequences = sequences;
            this.start = start;
            this.end = end;
        }
        /**
         * For serialization/deserialization only.
         */
        @SuppressWarnings("unused")
        private SequenceSubTaskMessage() {
        }
    }

    public static class LinearSubTaskMessage implements Serializable {

        private static final long serialVersionUID = 4926542426875360288L;
        private Map<String, Integer> passwords;
        private long start, end;

        public LinearSubTaskMessage(Map<String, Integer> passwords, long start, long end) {
            this.passwords = passwords;
            this.start = start;
            this.end = end;
        }
        /**
         * For serialization/deserialization only.
         */
        @SuppressWarnings("unused")
        private LinearSubTaskMessage() {
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
                .match(SequenceSubTaskMessage.class, this::handle)
                .match(LinearSubTaskMessage.class, this::handle)
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
//                    System.out.println("Match!");
                    cleartext = new HashMap<String, Integer>();
                    cleartext.put(key, i);
                    this.sender().tell(new Master.SecretRevealedMessage(cleartext), this.self());
                }

            }
        }
    }

    private void handle(SequenceSubTaskMessage message) {
        int start = message.start;
        int end = message.end;

        System.out.println("My SequenceRange: " + start + "-" + end);

        Map<String, String> sequences = message.sequences;
        Map<String, String> cleartext;
        String id;
        String sequence;
        int maxSubstringLength;
        String maxSubstringPartner;

        for (int i = start; i <= end; i++) {
            id = Integer.toString(i);
            sequence = sequences.get(id);
            maxSubstringLength = 0;
            maxSubstringPartner = "";
            int overlapSize;
            for (Map.Entry<String,String> entry : sequences.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if(!key.equals(id)) {
                    overlapSize = this.longestOverlap(value, sequence).length();
                    if (overlapSize > maxSubstringLength) {
                        maxSubstringLength = overlapSize;
                        maxSubstringPartner = key;
                    }
                }
            }
            cleartext = new HashMap<String, String>();
            cleartext.put(id, maxSubstringPartner);
            this.sender().tell(new Master.SequenceRevealedMessage(cleartext), this.self());
        }
    }


    private void handle(LinearSubTaskMessage message) {
        long start = message.start;
        long end = message.end;

        System.out.println("My LinearRange: " + start + "-" + end);

        Map<String, Integer> passwords = message.passwords;

        Map<String, Integer> cleartext = new HashMap<String, Integer>();

        for (long i = start; i <= end; i++) {
            int[] prefixes = this.binaryFromLong(i);
            int sum = 0;
            int idx = 0;
            for (Map.Entry<String, Integer> entry : passwords.entrySet()) {
//                String key = entry.getKey();
                sum += entry.getValue() * prefixes[idx];
                idx ++;
            }
            if (sum == 0) {
                idx = 0;
                for (Map.Entry<String, Integer> entry : passwords.entrySet()) {
//                String key = entry.getKey();
                    cleartext.put(entry.getKey(), prefixes[idx]);
                    idx ++;
                }
                break;
            }
        }

        this.sender().tell(new Master.LinearRevealedMessage(cleartext), this.self());
    }

    private int[] binaryFromLong(long number){
        String binary = Long.toBinaryString(number);
        int[] prefixes = new int[42];
        for (int i = 0; i < prefixes.length; i++)
            prefixes[i] = 1;

        int i = 0;
        for (int j = binary.length() - 1; j >= 0 && i < 42; j--) {
            if (binary.charAt(j) == '1')
                prefixes[i] = -1;
            i++;
        }
        return prefixes;
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

    private String longestOverlap(String str1, String str2) {
        if (str1.isEmpty() || str2.isEmpty())
            return "";

        if (str1.length() > str2.length()) {
            String temp = str1;
            str1 = str2;
            str2 = temp;
        }

        int[] currentRow = new int[str1.length()];
        int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
        int longestSubstringLength = 0;
        int longestSubstringStart = 0;

        for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
            char str2Char = str2.charAt(str2Index);
            for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
                int newLength;
                if (str1.charAt(str1Index) == str2Char) {
                    newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

                    if (newLength > longestSubstringLength) {
                        longestSubstringLength = newLength;
                        longestSubstringStart = str1Index - (newLength - 1);
                    }
                } else {
                    newLength = 0;
                }
                currentRow[str1Index] = newLength;
            }
            int[] temp = currentRow;
            currentRow = lastRow;
            lastRow = temp;
        }
        return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength);
    }










}