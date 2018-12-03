package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;

/**
 * @author xpleaf
 * @date 2018/12/3 6:11 PM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-document-update.html#java-rest-high-document-update-request
 *
 * The Update API allows to update an existing document by using a script or by passing a partial document.
 * 也就是可以用脚本或者部分文档来进行更新，写过更新的query DSL应该很容易理解了，如果不了解，可以参考我之前写的博客文章：
 * http://blog.51cto.com/xpleaf/2309518
 *
 * 基于这个文档进行操作：
 * {
 *   "_index": "posts",
 *   "_type": "doc",
 *   "_id": "1",
 *   "_version": 2,
 *   "found": true,
 *   "_source": {
 *     "user": "kimchy",
 *     "postDate": "2013-01-30",
 *     "message": "trying out Elasticsearch",
 *     "age": 13
 *   }
 * }
 */
public class _4UpdateAPI {

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

    // 使用script进行更新（inline方式，也就是内嵌脚本，当然也还有stored和file，不过这里不进行说明）
    // 不过需要注意的是，es 5.6，已经不再使用groovy脚本，而是使用painless脚本
    @Test
    public void test01() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("posts", "doc", "1");
        // 设置参数
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("count", 1);
        // 构建inline脚本
        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.age += params.count", parameters);
        updateRequest.script(inline);
        UpdateResponse updateResponse = client.update(updateRequest);
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功！");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("删除成功！");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            System.out.println("什么操作也没做！");
        }
    }

    // 基于json字符串进行更新
    @Test
    public void test02() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("posts", "doc", "1");
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update\"" +
                "}";
        // 设置doc，并指定类型为json字符串
        updateRequest.doc(jsonString, XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest);
        if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
    }

    // 基于map进行更新
    @Test
    public void test03() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("posts", "doc", "1");
        HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated", new Date());
        jsonMap.put("reason", "daily updated");
        // 直接设置doc
        updateRequest.doc(jsonMap);
        UpdateResponse updateResponse = client.update(updateRequest);
        if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
    }

    // 基于jsonBuilder进行更新
    @Test
    public void test04() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("posts", "doc", "1");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("updated", new Date());
            builder.field("reason", "daily updated");
        }
        builder.endObject();
        // 设置doc
        updateRequest.doc(builder);
        UpdateResponse updateResponse = client.update(updateRequest);
        if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
    }

    // 基于key-pairs进行更新
    @Test
    public void test05() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("posts", "doc", "1");
        // 设置doc
        updateRequest.doc("updated", new Date(),
                          "reason", "daily updated");
        UpdateResponse updateResponse = client.update(updateRequest);
        if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
    }

    // upsert操作，如果document存在，则更新，不存在则写入该文档
    @Test
    public void test06() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("posts", "doc", "2");
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update2\"" +
                "}";
        // 如果不存在，则写入
        updateRequest.upsert(jsonString, XContentType.JSON);
        // 否则执行更新
        updateRequest.doc(jsonString, XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest);
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功！");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("删除成功！");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            System.out.println("什么操作也没做！");
        }
    }

    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
