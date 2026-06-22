package com.example.kafka.orderanalyticsservice.service;

import com.example.kafka.orderanalyticsservice.model.ProductSales;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads the live aggregate directly out of the Kafka Streams state store using
 * <b>interactive queries</b>.
 *
 * <p>This is the key difference from the MongoDB read model: there is no external database. The
 * {@code KTable} aggregate lives inside this running process (a RocksDB-backed state store, fronted
 * by an in-memory cache and backed up to a Kafka changelog topic). We query that store directly.</p>
 */
@Service
public class ProductSalesQueryService {

    private final StreamsBuilderFactoryBean factoryBean;
    private final String storeName;

    public ProductSalesQueryService(StreamsBuilderFactoryBean factoryBean,
                                    @Value("${app.store.product-sales}") String storeName) {
        this.factoryBean = factoryBean;
        this.storeName = storeName;
    }

    public List<ProductSales> findAll() {
        ReadOnlyKeyValueStore<String, ProductSales> store = store();
        List<ProductSales> result = new ArrayList<>();
        try (KeyValueIterator<String, ProductSales> it = store.all()) {
            it.forEachRemaining(entry -> result.add(entry.value));
        }
        return result;
    }

    public Optional<ProductSales> findByProduct(String product) {
        return Optional.ofNullable(store().get(product));
    }

    /**
     * Resolves the queryable store. The store is only available once the streams engine has reached
     * the RUNNING state and finished restoring; until then we surface a clear error.
     */
    private ReadOnlyKeyValueStore<String, ProductSales> store() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException("Kafka Streams state store is not ready yet. Try again shortly.");
        }
        return streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }
}
