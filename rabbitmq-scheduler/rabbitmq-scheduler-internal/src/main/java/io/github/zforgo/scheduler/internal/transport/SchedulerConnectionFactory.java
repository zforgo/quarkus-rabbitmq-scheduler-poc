package io.github.zforgo.scheduler.internal.transport;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import com.rabbitmq.client.ConnectionFactory;

import io.quarkus.tls.TlsConfigurationRegistry;

import io.github.zforgo.scheduler.SchedulerConfig;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class SchedulerConnectionFactory {

    public static ConnectionFactory create(
            SchedulerConfig.BrokerConfig config, Optional<TlsConfigurationRegistry> tlsRegistry
    ) {
        var factory = new ConnectionFactory();

        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setVirtualHost(config.virtualHost());
        config.username().ifPresent(factory::setUsername);
        config.password().ifPresent(factory::setPassword);
        factory.setNetworkRecoveryInterval(config.networkRecoveryInterval().toMillis());
        factory.setConnectionTimeout(durationToInt(config::connectionTimeout));
        factory.setHandshakeTimeout(durationToInt(config::handshakeTimeout));
        factory.setRequestedHeartbeat(durationToInt(config::requestedHeartbeat));

        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);

        configureTls(factory, config, tlsRegistry);
        return factory;
    }

    private static void configureTls(
            ConnectionFactory f, SchedulerConfig.BrokerConfig config, Optional<TlsConfigurationRegistry> registry
    ) {
        var tlsName = config.tlsConfigurationName();

        try {
            if (tlsName.isPresent()) {
                var tls = registry.flatMap(r -> r.get(tlsName.get()))
                        .orElseThrow(() -> new IllegalStateException("Unknown TLS configuration: " + tlsName.get()));
                f.useSslProtocol(tls.createSSLContext());
            } else if (config.ssl()) {
                f.useSslProtocol(SSLContext.getDefault()); // NEVER use no-arg useSslProtocol()
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot configure RabbitMQ TLS", e);
        }
    }

    private static int durationToInt(Duration duration) {
        return Long.valueOf(duration.toMillis()).intValue();
    }

    private static int durationToInt(Supplier<Duration> durationSupplier) {
        return durationToInt(durationSupplier.get());
    }
}
