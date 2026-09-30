package local.home.arpa.telemetry.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** Fetches the current schema for a subject from the ccompat-compatible registry (Apicurio). */
public final class SchemaRegistryLookup {

    private SchemaRegistryLookup() {}

    public static Schema fetchLatest(String registryUrl, String subject) throws Exception {
        String url = registryUrl + "/subjects/" + subject + "/versions/latest";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IllegalStateException(
                    "Could not fetch schema for subject '" + subject + "' from " + url
                            + " (HTTP " + response.statusCode() + "): " + response.body());
        }

        JsonNode root = new ObjectMapper().readTree(response.body());
        return new Schema.Parser().parse(root.get("schema").asText());
    }
}
