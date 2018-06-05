package com.fairbit.commonservices.commonservices;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.kubernetes.KubernetesServiceImporter;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.servicediscovery.types.HttpEndpoint;
import io.vertx.servicediscovery.types.JDBCDataSource;
import io.vertx.servicediscovery.types.MessageSource;

import java.util.*;


/**
 * Base Vertilce with Circuit-Breaker!
 */
public class BaseVerticle extends AbstractVerticle {
	private static final String LOG_EVENT_ADDRESS = "events.log";
	protected ServiceDiscovery serviceDiscovery;
	protected CircuitBreaker circuitBreaker;
	protected Set<Record> registeredRecords = new ConcurrentHashSet<>();

	private static final Logger logger = LoggerFactory.getLogger(BaseVerticle.class);

	@Override
	public void start() throws Exception {


		serviceDiscovery = ServiceDiscovery.create(vertx);
		serviceDiscovery.registerServiceImporter(new KubernetesServiceImporter(), config().copy().put("namespace", "default"));

		circuitBreaker = CircuitBreaker.create("api-gateway-cb", vertx, new CircuitBreakerOptions()
				.setMaxFailures(5)
				.setTimeout(10000L)
				.setFallbackOnFailure(true)
				.setResetTimeout(30000L)
		);


	}

	protected Future<Void> publishHttpEndpoint(String name, String host, int port, String apiname) {
		Record record = HttpEndpoint.createRecord(name, host, port, "/",
				new JsonObject().put("api.name", apiname));
		System.out.println(" Published record " + record.toString());
		return publish(record);
	}

	protected Future<Void> publishApiGateway(String host, int port) {
		Record record = HttpEndpoint.createRecord("api-gateway", true, host, port, "/", null)
				.setType("api-gateway");
		return publish(record);
	}

	protected Future<Void> publishMessageSource(String name, String address) {
		Record record = MessageSource.createRecord(name, address);
		return publish(record);
	}

	protected Future<Void> publishJDBCDataSource(String name, JsonObject location) {
		Record record = JDBCDataSource.createRecord(name, location, new JsonObject());
		return publish(record);
	}

	protected Future<Void> publishEventBusService(String name, String address, Class serviceClass) {
		Record record = EventBusService.createRecord(name, address, serviceClass);
		return publish(record);
	}

	private Future<Void> publish(Record record) {
		if (serviceDiscovery == null) {
			try {
				start();
			} catch (Exception e) {
				throw new IllegalStateException("Cannot create discovery service");
			}
		}
		Future<Void> future = Future.future();
		// publish the service
//        System.out.println(record.toJson().encodePrettily());

		serviceDiscovery.publish(record, ar -> {
			System.out.println(record.toJson());
			if (ar.succeeded()) {
				registeredRecords.add(record);
				System.out.println("Service <" + ar.result().getName() + "> published. ");
				logger.info("Service <" + ar.result().getName() + "> published");
				future.complete();
			} else {
				future.fail(ar.cause());
			}
		});
		return future;
	}

	protected void publishLogEvent(String type, JsonObject data) {
		JsonObject msg = new JsonObject().put("type", type)
				.put("message", data);
		vertx.eventBus().publish(LOG_EVENT_ADDRESS, msg);
	}

	protected void publishLogEvent(String type, JsonObject data, boolean succeeded) {
		JsonObject msg = new JsonObject().put("type", type)
				.put("status", succeeded)
				.put("message", data);
		vertx.eventBus().publish(LOG_EVENT_ADDRESS, msg);
	}


	@Override
	public void stop(Future<Void> stopFuture) throws Exception {
		List<Future> futures = new ArrayList<>();
		registeredRecords.forEach(record -> {
			Future<Void> cleanupFuture = Future.future();
			futures.add(cleanupFuture);
			serviceDiscovery.unpublish(record.getRegistration(), cleanupFuture.completer());
		});

		if (futures.isEmpty()) {
			serviceDiscovery.close();
			stopFuture.complete();
		} else {
			CompositeFuture.all(futures)
					.setHandler(ar -> {
						serviceDiscovery.close();
						if (ar.failed()) {
							stopFuture.fail(ar.cause());
						} else {
							stopFuture.complete();
						}
					});
		}
	}
}

