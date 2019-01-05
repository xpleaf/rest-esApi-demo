package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2018/11/30 10:27 PM
 *
 * index操作请参考Jest Index API
 */
public class _1IndexAPI {

    // rest-low-level-client
    RestClient restLowLevelClient = null;
    // rest-high-level-client
    RestHighLevelClient client = null;

    @Before
    public void init() throws Exception {
        restLowLevelClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
                // new HttpHost("localhost", 9201, "http")
                // more hosts and ports
        ).build();
        client = new RestHighLevelClient(restLowLevelClient);
    }

    // 创建index
    @Test
    public void test01() throws Exception {

    }

    // 使用json字符串创建
    @Test
    public void test02() throws Exception {
        IndexRequest request = new IndexRequest(
                "posts",
                "doc",
                "1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"," +
                "\"age\":13" +
                "}";
        request.source(jsonString, XContentType.JSON);
        client.index(request);
    }

    // 使用对象创建，同时不指定id，由es自动生成id
    @Test
    public void test03() throws Exception {
        IndexRequest indexRequest = new IndexRequest("posts", "doc");
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("user", "xpleaf");
        dataMap.put("postDate", "2019-01-05");
        dataMap.put("message", "Never stop trying.");
        dataMap.put("age", 25);
        indexRequest.source(dataMap);
        IndexResponse indexResponse = client.index(indexRequest);
        RestStatus status = indexResponse.status();
        System.out.println(status);
    }

    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
