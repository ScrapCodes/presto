/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.cost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class BayesCardPythonCallerAPI
{
    private final String filterPredicate;
    private final String joinPredicate;

    BayesCardPythonCallerAPI(String filterPredicate, String joinPredicate)
    {
        this.filterPredicate = filterPredicate;
        this.joinPredicate = joinPredicate;
    }

    private static String formatJsonString(String value)
    {
        return value == null ? null : String.format("\"%s\"", value);
    }

    public float callAPI()
            throws Exception
    {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // Create an HttpPost request
            HttpPost httpPost = new HttpPost("http://127.0.0.1:5000/run_experiment_imdb");

            // Set the Content-Type header to application/json
            httpPost.setHeader("Content-Type", "application/json");

            // Set the JSON body
            String request = String.format("{\"join_condition\":%s, \"predicate\":%s}", formatJsonString(joinPredicate), formatJsonString(filterPredicate));
            httpPost.setEntity(new StringEntity(request));

            // Execute the request
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();
                String result = EntityUtils.toString(entity);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(result);
                String status = rootNode.path("status").asText();

                // Check if status is "success"
                if ("success".equals(status)) {
                    // Get result as float
                    return (float) rootNode.path("result").asDouble();
                }
                else {
                    System.out.println(request);
                    throw new RuntimeException("Error: Status is not success");
                }
            }
        }
    }
}
