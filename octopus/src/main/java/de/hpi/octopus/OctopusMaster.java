package de.hpi.octopus;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import de.hpi.octopus.actors.Master;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;

import java.util.Scanner;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		final ActorSystem system = createSystem(actorSystemName, config);


		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				system.actorOf(Master.props(), Master.DEFAULT_NAME);
				
				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);


                /**
                 * TODO
                 */


			}
		});


		/**
		final Scanner scanner = new Scanner(System.in);
		String line = scanner.nextLine();
		scanner.close();
		
		int attributes = Integer.parseInt(line);
		
		system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.TaskMessage(attributes), ActorRef.noSender());
        /**







	}
}
