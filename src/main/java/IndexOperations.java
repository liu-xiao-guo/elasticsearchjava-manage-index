import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.json.JsonpMapper;
import jakarta.json.Json;
import jakarta.json.stream.JsonParser;

import java.io.IOException;
import java.io.StringReader;

public class IndexOperations {
    private final ElasticsearchClient client;
    public IndexOperations(ElasticsearchClient client)
        { this.client = client; }

    // Check whether an index exists or not
    public boolean checkIndexExists(String name) throws IOException {
        return client.indices().exists(c -> c.index(name)).value();
    }

    // Create an index if it does not exist
    public void createIndex(String name) throws IOException {
        client.indices().create(c -> c.index(name));
    }

    // Delete an index if it exists
    public void deleteIndex(String name) throws IOException {
        client.indices().delete(c -> c.index(name));
    }

    // Close an index
    public void closeIndex(String name) throws IOException {
        client.indices().close(c -> c.index(name));
    }

    // Open an index
    public void openIndex(String name) throws IOException {
        client.indices().open(c -> c.index(name));
    }

    // Create an index with mappings defined
    public void putMapping(String index, String mappings) throws IOException {
        JsonpMapper mapper = client._transport().jsonpMapper();
        JsonParser parser = Json.createParser(new StringReader(mappings));
        CreateIndexRequest request_create =  new CreateIndexRequest.Builder()
                .index(index)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper))
                .build();
        CreateIndexResponse response_create = client.indices().create(request_create);
    }
}
