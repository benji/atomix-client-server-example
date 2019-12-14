package com.github.benji.atomix;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.map.AtomicMap;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.protocols.raft.partition.RaftStorageConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;

public class AtomixClientServerTest {

	private final static String ATOMIX_CLUSTER_ID = "test";
	private final static String ATOMIX_MGMT_PARTITION_GROUP_NAME = "system";
	private final static String ATOMIX_DATA_PARTITION_GROUP_NAME = "data";

	protected static final int BASE_PORT = 5000;
	protected static final File DATA_DIR = new File(System.getProperty("user.dir"), ".data");
	private List<Atomix> instances;

	List<Integer> serverIds = Arrays.asList(1, 2, 3);

	Collection<Node> nodes = serverIds.stream()
			.map(memberId -> Node.builder()
					.withId(String.valueOf(memberId))
					.withAddress(Address.from("localhost", BASE_PORT + memberId))
					.build())
			.collect(Collectors.toList());

	@Before
	public void setupInstances() throws Exception {
		instances = new ArrayList<>();
	}

	@After
	public void teardownInstances() throws Exception {
		List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::stop).collect(Collectors.toList());
		try {
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
		} catch (Exception e) {
			// Do nothing
		}
	}

	@Test
	public void testClientJoinLeaveConsensus() throws Exception {
		// create 3 servers
		List<CompletableFuture<Atomix>> futures = new ArrayList<>();
		futures.add(startAtomixServer(1));
		futures.add(startAtomixServer(2));
		futures.add(startAtomixServer(3));
		Futures.allOf(futures).get(30, TimeUnit.SECONDS);

		// create 2 clients
		Atomix client1 = startAtomixClient(4).get(30, TimeUnit.SECONDS);
		Atomix client2 = startAtomixClient(5).get(30, TimeUnit.SECONDS);

		// put in distributed map from client1
		AtomicMap m = client1.atomicMapBuilder("my-map").build();
		m.put("hello", "world");

		// get from distributed map from client2
		Object o = client2.getAtomicMap("my-map").get("hello").value();
		assertEquals("world", o);
	}

	private CompletableFuture<Atomix> startAtomixServer(int id) {
		AtomixBuilder b = Atomix.builder()
				.withClusterId(ATOMIX_CLUSTER_ID)
				.withMemberId(String.valueOf(id))
				.withHost("localhost")
				.withPort(BASE_PORT + id)
				.withMembershipProvider(new BootstrapDiscoveryProvider(nodes));

		b.withManagementGroup(new RaftPartitionGroup(new RaftPartitionGroupConfig()
				.setName(ATOMIX_MGMT_PARTITION_GROUP_NAME)
				.setPartitionSize(3)
				.setPartitions(1)
				.setMembers(serverIds.stream().map(i -> String.valueOf(i)).collect(Collectors.toSet()))
				.setStorageConfig(new RaftStorageConfig()
						.setDirectory(tempTestDirectory()))));

		b.withPartitionGroups(PrimaryBackupPartitionGroup.builder(ATOMIX_DATA_PARTITION_GROUP_NAME)
				.withNumPartitions(1)
				.build());

		Atomix atomix = b.build();
		instances.add(atomix);
		return atomix.start().thenApply(v -> atomix);
	}

	private String tempTestDirectory() {
		return new File(DATA_DIR, UUID.randomUUID().toString()).getAbsolutePath();
	}

	private CompletableFuture<Atomix> startAtomixClient(int id) {
		AtomixBuilder b = Atomix.builder()
				.withClusterId(ATOMIX_CLUSTER_ID)
				.withMemberId(String.valueOf(id))
				.withHost("localhost")
				.withPort(BASE_PORT + id)
				.withMembershipProvider(new BootstrapDiscoveryProvider(nodes));

//		b.withPartitionGroups(PrimaryBackupPartitionGroup.builder("data")
//				.withNumPartitions(1)
//				.build());

		Atomix atomix = b.build();
		instances.add(atomix);
		return atomix.start().thenApply(v -> atomix);
	}

}
