package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
 *
 * Note:
 * The Bulk API supports only documents encoded in JSON or SMILE.
 * Providing documents in any other format will result in an error.
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

    // bulkResponse失败处理
    @Test
    public void test03() throws Exception {
        // 构建BulkRequest请求
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest("posts", "doc", "3"));
        request.add(new UpdateRequest("posts", "doc", "2")
                .doc(XContentType.JSON,"other", "test"));
        request.add(new IndexRequest("posts", "doc", "4")
                .source(XContentType.JSON,"field", "baz"));
        BulkResponse bulkResponse = client.bulk(request);
        // 处理可能会有的失败
        if(bulkResponse.hasFailures()) {    // 说明至少有一个请求失败了
            for(BulkItemResponse bulkItemResponse : bulkResponse) { // 遍历每一个结果，看哪一个失败了
                if(bulkItemResponse.isFailed()) {
                    // 获取失败的异常
                    Exception e = bulkItemResponse.getFailure().getCause();
                    e.printStackTrace();
                    throw e;
                }

            }
        }
    }

    /**
     * 使用Bulk Processor API在批量操作完成之前和之后进行相应的操作
     * 构建BulkProcessor需要RestHighLevelClient、BulkProcessor.Listener和ThreadPool
     * ThreadPool在test case下无法创建，所以这里使用main函数
     */
//    @Test
//    public void test04() throws Exception {
    public static void main(String[] args) throws Exception {
        // rest-low-level-client
        RestClient restLowLevelClient = null;
        // rest-high-level-client
        RestHighLevelClient client = null;

        restLowLevelClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
                // new HttpHost("localhost", 9201, "http")
                // more hosts and ports
        ).build();
        client = new RestHighLevelClient(restLowLevelClient);

        // 构建ThreadPool
        Settings settings = Settings.builder().build();
        ThreadPool threadPool = new ThreadPool(settings);
        /**
         * 构建BulkProcessor.Listener
         * 注意每一个批次的操作都会执行Listener中的方法
         * 比如设置了BulkActions为2，那么每两个请求，都会执行Listener中的方法
         */
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            // 批量操作前
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("---批量操作前---");
                int numberOfActions = request.numberOfActions();
                System.out.println(String.format("numberOfActions: %s", numberOfActions));
                // 设置超时时间为1分钟
                request.timeout(TimeValue.timeValueMinutes(1));
                System.out.println("---批量操作前---");
            }

            // 批量操作后
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("---批量操作后---");
                int status = response.status().getStatus();
                System.out.println(String.format("status: %s", status));
                /**
                 * 下面的操作不是必需的，在这里只是告诉读者，批量操作时，可能会出现某几请求失败的情况
                 * 因此这里是告诉读者一种方式，即如何找到失败的请求，之后可以再根据自己的策略进行重试
                 */
                // 同时可以获取到请求
                List<DocWriteRequest> requests = request.requests();
                // 并获取到对应的response
                BulkItemResponse[] bulkItemResponses = response.getItems();
                // 可以找到失败的请求
                int successCount = 0;   // 统计成功的请求数
                if(response.hasFailures()) {    // 说明至少有一个请求失败了
                    // 遍历找到失败的请求，和与之对应的request，同时统计成功的请求数
                    for(int i = 0; i < bulkItemResponses.length; i++) {
                        BulkItemResponse bulkItemResponse = bulkItemResponses[i];
                        if (bulkItemResponse.isFailed()) {
                            // 拿到失败的请求，其顺序与bulkItemResponses数组是一一对应的
                            DocWriteRequest docWriteRequest = requests.get(i);
                            // 判断各个请求所属的请求类型
                            if(docWriteRequest instanceof IndexRequest) {
                                IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                                System.out.println("创建请求失败了！");
                                // TODO 创建请求失败处理策略
                            } else if(docWriteRequest instanceof UpdateRequest) {
                                UpdateRequest updateRequest = (UpdateRequest) docWriteRequest;
                                System.out.println("更新请求失败了！");
                                // TODO 更新请求失败处理策略
                            } else if(docWriteRequest instanceof DeleteRequest) {
                                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                                System.out.println("删除请求失败了！");
                                // TODO 删除请求失败处理策略
                            }
                            continue;
                        }
                        successCount++;
                    }
                } else {
                    successCount = bulkItemResponses.length;
                }
                System.out.println(String.format("成功请求数量：%s，消耗时间：%s ms", successCount, response.getTook().getMillis()));
                System.out.println("---批量操作后---");
            }

            // 批量操作出现异常时
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("---批量操作出现异常了---");
                failure.printStackTrace();
                System.out.println("---批量操作出现异常了---");
            }
        };
        // 构建BulkProcessor，同时设置相关参数
        BulkProcessor.Builder builder = new BulkProcessor.Builder(client::bulkAsync, listener, threadPool);
        BulkProcessor bulkProcessor = builder
                .setBulkActions(2)                                           // 设置请求操作的数据超过2次触发批量提交操作，默认为1000
                .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB))   // 设置批处理请求达到20M触发批量提交动作，默认为5MB
                .setFlushInterval(TimeValue.timeValueSeconds(5))            // 设置刷新索引时间间隔，默认没有设置，所以需要依赖调用processor.flush()
                .setConcurrentRequests(5)                                   // 设置并发处理线程个数
                .setBackoffPolicy(BackoffPolicy
                        .exponentialBackoff(TimeValue.timeValueMillis(100),
                                3))                      // 设置回滚策略，等待时间为100ms，retry次数为3次
                .build();
        // 构建多个请求
        IndexRequest one = new IndexRequest("posts", "doc", "11").
                source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts", "doc", "12")
                .source(XContentType.JSON, "title", "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts", "doc", "13")
                .source(XContentType.JSON, "title", "The Future of Federated Search in Elasticsearch");
        // 添加到bulkProcessor
        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);

        // 等待关闭，必须要等待关闭，否则请求还没有执行程序就退出了
        boolean close = bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
        if(close) {
            System.out.println("等待关闭bulkProcessor成功！");
        }

        // 关闭processor
        // bulkProcessor.close();

        // 必须要主动关闭restLowLevelClient，否则线程会一直处于等待状态
        restLowLevelClient.close();
    }


    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
