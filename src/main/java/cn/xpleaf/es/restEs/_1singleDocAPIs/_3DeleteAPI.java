package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xpleaf
 * @date 2018/12/3 5:26 PM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-document-delete.html
 */
public class _3DeleteAPI {

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

    // 简单删除
    @Test
    public void test01() throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest("posts", "doc", "1");
        DeleteResponse deleteResponse = client.delete(deleteRequest);
        // 获取deleteResponse中的结果
        String index = deleteResponse.getIndex();
        String type = deleteResponse.getType();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        if(deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("not found!");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        } else {
            System.out.println("删除成功！");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        }
    }

    // 设置等待超时
    @Test
    public void test02() throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest("posts", "doc", "1");
        // 设置超时时间为2分钟
        deleteRequest.timeout(TimeValue.timeValueMinutes(2));
        //deleteRequest.timeout("2m");                            // 也可以通过字符串的方式进行设置
        /**
         * 设置refresh策略
         * what is refresh策略？我先说添加数据
         * 数据是先写入到内存buffer，默认情况下，每隔一秒就生成一个新的index segment，index segment就是用来被搜索的
         * 同时数据写入segment后，也会写入到os cache，之后才会刷新到disk，想像一下，如果刷新到disk才可以被搜索，那么写入
         * 一个文档之后就会等待比较长的时间才被搜索到，显然这样就连近实时都不是了。更详细的，可以自己去了解一下，也可以来问我。
         *
         * OK，其实这里refresh策略，也就是删除的意思也是一样的，就是这个请求发送出去之后，等到它不可以被搜索到了，才返回结果，
         * 这里的WriteRequest.RefreshPolicy.WAIT_UNTIL指的就是这个意思
         * Leave this request open until a refresh has made the contents of this request visible to search. This refresh policy is
         * compatible with high indexing and search throughput but it causes the request to wait to reply until a refresh occurs.
         */
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //deleteRequest.setRefreshPolicy("wait_for");
        DeleteResponse deleteResponse = client.delete(deleteRequest);
        // 获取deleteResponse中的结果
        String index = deleteResponse.getIndex();
        String type = deleteResponse.getType();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        if(deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("not found!");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        } else {
            System.out.println("删除成功！");
            System.out.println(String.format("index: %s, type: %s, id: %s, version: %s", index, type, id, version));
        }
    }

    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
