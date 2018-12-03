package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author xpleaf
 * @date 2018/12/3 3:53 PM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-document-get.html
 */
public class _2GetAPI {

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

    // 简单的get请求-response包含source数据
    @Test
    public void test01() throws Exception {
        GetRequest getRequest = new GetRequest("posts", "doc", "1");
        GetResponse getResponse = client.get(getRequest);
        if(getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            System.out.println(String.format("version: %s, \nsourceAsString: %s, \nsourceAsMap: %s", version, sourceAsString, sourceAsMap));
        }
    }

    // 简单的get请求-response不包含source数据
    @Test
    public void test02() throws Exception {
        GetRequest getRequest = new GetRequest("posts", "doc", "1");
        // 设置FetchSourceContext属性
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        GetResponse getResponse = client.get(getRequest);
        if(getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();        // null
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap(); // null
            System.out.println(String.format("version: %s, \nsourceAsString: %s, \nsourceAsMap: %s", version, sourceAsString, sourceAsMap));
        }
    }

    // 只返回特定字段的数据
    @Test
    public void test03() throws Exception {
        GetRequest getRequest = new GetRequest("posts", "doc", "1");
        // 指定返回的字段
        String[] includes = new String[]{"message", "*Date"};
        // 不需要返回的字段设置为空，说明只要返回includes包含的字段就好了
        String[] excludes = Strings.EMPTY_ARRAY;    // 其实就是 new String[0]
        // 设置FetchSourceContext属性
        getRequest.fetchSourceContext(new FetchSourceContext(true, includes, excludes));
        GetResponse getResponse = client.get(getRequest);
        if(getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            System.out.println(String.format("version: %s, \nsourceAsString: %s, \nsourceAsMap: %s", version, sourceAsString, sourceAsMap));
        }
    }

    // 过滤掉特定字段的数据
    @Test
    public void test04() throws Exception {
        GetRequest getRequest = new GetRequest("posts", "doc", "1");
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"message"};
        getRequest.fetchSourceContext(new FetchSourceContext(true, includes, excludes));
        GetResponse getResponse = client.get(getRequest);
        if(getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            System.out.println(String.format("version: %s, \nsourceAsString: %s, \nsourceAsMap: %s", version, sourceAsString, sourceAsMap));
        }
    }

    // getRequest还有其它很多参数设置，比如可以设置version等，可以参考前面提供的官方文档
    // 并且es如何基于version进行乐观锁并发控制，这方面可以自己去查找一下相关资料，这里就不介绍说明了

    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
