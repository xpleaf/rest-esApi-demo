package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xpleaf
 * @date 2018/12/4 11:42 AM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-document-bulk.html#java-rest-high-document-bulk
 * Bulk Processor参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-document-bulk.html#java-rest-high-document-bulk-processor
 * Bulk Processor的使用案例也可以参考：
 * https://github.com/xpleaf/es-java-api/blob/master/src/main/java/cn/xpleaf/es/_3doc/_4TestBulkAPI.java
 */
public class _5BulkAPI {

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

    // 只包含单一操作的bulkRequest测试
    @Test
    public void test01() throws Exception {
        // 构建BulkRequest请求
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("posts", "doc", "1")
                    .source(XContentType.JSON, "field", "foo"));
        bulkRequest.add(new IndexRequest("posts", "doc", "2")
                .source(XContentType.JSON, "field", "bar"));
        bulkRequest.add(new IndexRequest("posts", "doc", "3")
                .source(XContentType.JSON, "field", "baz"));
        BulkResponse bulkResponse = client.bulk(bulkRequest);
        // 处理返回结果
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                // 创建操作
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                if(indexResponse.status() == RestStatus.CREATED || indexResponse.status() == RestStatus.OK) {
                    String index = indexResponse.getIndex();
                    String type = indexResponse.getType();
                    String id = indexResponse.getId();
                    System.out.println("创建成功！");
                    System.out.println(String.format("index: %s, type: %s, id: %s", index, type, id));
                }
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                // 更新操作
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    String index = updateResponse.getIndex();
                    String type = updateResponse.getType();
                    String id = updateResponse.getId();
                    System.out.println("更新成功！");
                    System.out.println(String.format("index: %s, type: %s, id: %s", index, type, id));
                }
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                // 删除操作
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                if(deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                    String index = deleteResponse.getIndex();
                    String type = deleteResponse.getType();
                    String id = deleteResponse.getId();
                    System.out.println("删除成功！");
                    System.out.println(String.format("index: %s, type: %s, id: %s", index, type, id));
                }
            } else {
                DocWriteResponse.Result result = itemResponse.getResult();
                System.out.println(result);
            }
        }
    }

    // 包含不同操作的bulkRequest测试
    @Test
    public void test02() throws Exception {
        // 构建BulkRequest请求
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest("posts", "doc", "3"));
        request.add(new UpdateRequest("posts", "doc", "2")
                .doc(XContentType.JSON,"other", "test"));
        request.add(new IndexRequest("posts", "doc", "4")
                .source(XContentType.JSON,"field", "baz"));
        BulkResponse bulkResponse = client.bulk(request);
        // 处理返回结果
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                // 创建操作
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                if(indexResponse.status() == RestStatus.CREATED || indexResponse.status() == RestStatus.OK) {
                    String index = indexResponse.getIndex();
                    String type = indexResponse.getType();
                    String id = indexResponse.getId();
                    System.out.println("创建成功！");
                    System.out.println(String.format("index: %s, type: %s, id: %s", index, type, id));
                }
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                // 更新操作
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                if(updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    String index = updateResponse.getIndex();
                    String type = updateResponse.getType();
                    String id = updateResponse.getId();
                    System.out.println("更新成功！");
                    System.out.println(String.format("index: %s, type: %s, id: %s", index, type, id));
                }
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                // 删除操作
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                if(deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                    String index = deleteResponse.getIndex();
                    String type = deleteResponse.getType();
                    String id = deleteResponse.getId();
                    System.out.println("删除成功！");
                    System.out.println(String.format("index: %s, type: %s, id: %s", index, type, id));
                }
            } else {
                DocWriteResponse.Result result = itemResponse.getResult();
                System.out.println(result);
            }
        }
    }

    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
