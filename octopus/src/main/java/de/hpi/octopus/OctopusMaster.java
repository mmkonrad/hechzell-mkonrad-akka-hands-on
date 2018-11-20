package de.hpi.octopus;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import de.hpi.octopus.actors.Master;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;
import scala.concurrent.Await;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port, String inputFile) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		final ActorSystem system = createSystem(actorSystemName, config);





		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				system.actorOf(Master.props(), Master.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);

                List<String> names = new ArrayList<>(42);
                List<String> secrets = new ArrayList<>(42);
                List<String> sequences = new ArrayList<>(42);

                Map<String, String> secretsMap = new HashMap<String, String>(42);
                Map<String, String> sequenceMap = new HashMap<String, String>(42);

                Path filePath = new File(inputFile).toPath();
                Charset charset = Charset.defaultCharset();
                List<String> stringList = null;
                try {
                    stringList = Files.readAllLines(filePath, charset);
                } catch (IOException e) {
                    System.out.println("[ERROR] Input file not found: " + inputFile);
                    e.printStackTrace();
                }
                stringList.remove(0);
                String[] stringArray = stringList.toArray(new String[]{});

                for (String line : stringArray) {
                    if(line.length() > 0) {
                        String[] lineSplit = line.split(";");
                        names.add(lineSplit[1]);
                        secrets.add(lineSplit[2]);
                        sequences.add(lineSplit[3]);

                        secretsMap.put(lineSplit[0], lineSplit[2]);
                        sequenceMap.put(lineSplit[0], lineSplit[3]);
                    }
                }

                /* Here have the data */
                //System.out.println(names);
                //System.out.println(secrets);
                //System.out.println(sequences);


                /**
                for (Map.Entry<String,String> entry : secretsMap.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    System.out.println(key + ": " + value);
                }
                **/


                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
                system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.SecretsTaskMessage(secretsMap), ActorRef.noSender());


                /**
                 * try ask pattern

                Timeout timeout = new Timeout(100, TimeUnit.MILLISECONDS);
                ActorSelection actorSelection = system.actorSelection("/user/" + Master.DEFAULT_NAME.toString());
                Future<Object> future = Patterns.ask(actorSelection, new Master.HashesToCrackMessage(secretsMap), timeout);
                ActorIdentity reply = (ActorIdentity) Await.result(future, timeout.duration());
                **/

            }
		});


		/**
		final Scanner scanner = new Scanner(System.in);
		String line = scanner.nextLine();
		scanner.close();
		
		int attributes = Integer.parseInt(line);
		
		system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.TaskMessage(attributes), ActorRef.noSender());
        **/







	}
}
