package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xpleaf
 * @date 2018/12/12 2:13 PM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-search-scroll.html#java-rest-high-search-scroll
 *
 * 为什么要使用scroll？
 * 解决分页查询的deep paging问题，下次会写文章分析一下这个问题，篇幅问题，这里只引出，不做分析和解析。
 */
public class _7SearchScrollAPI {

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

    // 以scroll方式查询数据，只获取scrollId和第一次查询的数据
    @Test
    public void test01() throws Exception {
        // 构建searchRequest
        SearchRequest searchRequest = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设置查询条件为匹配所有
        searchSourceBuilder.size(2);    // 每次返回2条数据
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        // 设置scroll属性
        searchRequest.scroll(TimeValue.timeValueMinutes(1));    // 设置时间窗口为1分钟，这意味着es会保持后面返回的scrollId对应的句柄为1分钟
        // 查询
        SearchResponse searchResponse = client.search(searchRequest);

        String scrollId = searchResponse.getScrollId();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();

        System.out.println(String.format("TotalHits: %s, ScrollId: %s", totalHits, scrollId));
        // 遍历输出结果
        hits.forEach(hit -> {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        });

        // 清理scroll
        /**
         * The search contexts used by the Search Scroll API are automatically deleted when the scroll times out.
         * But it is advised to release search contexts as soon as they are not necessary anymore using the Clear Scroll API.
         */
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        if(clearScrollResponse.isSucceeded()) {
            System.out.println(String.format("清理scrollId: %s 成功!", scrollId));
        }
    }

    // 通过scrollId查询第二批数据
    @Test
    public void test02() throws Exception {
        // 构建searchRequest
        SearchRequest searchRequest = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设置查询条件为匹配所有
        searchSourceBuilder.size(2);    // 每次返回2条数据
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        // 设置scroll属性
        searchRequest.scroll(TimeValue.timeValueMinutes(1));    // 设置时间窗口为1分钟，这意味着es会保持后面返回的scrollId对应的句柄为1分钟
        // 1.查询，获取第一秕次查询结果
        SearchResponse searchResponse = client.search(searchRequest);

        String scrollId = searchResponse.getScrollId();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();

        System.out.println(String.format("获取第一批次数据：TotalHits: %s, ScrollId: %s", totalHits, scrollId));
        // 遍历输出结果
        hits.forEach(hit -> {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        });

        // 2.获取第二批次数据，需要通过SearchScrollRequest来构建查询对象
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(30));
        SearchResponse searchScrollResponse = client.searchScroll(scrollRequest);

        // 处理结果
        String sameScrollId = searchScrollResponse.getScrollId();
        SearchHits hits1 = searchScrollResponse.getHits();
        long totalHits1 = hits1.getTotalHits();

        System.out.println(String.format("获取第二批次数据：TotalHits: %s, ScrollId: %s", totalHits1, sameScrollId));
        // 遍历输出结果
        hits1.forEach(hit -> {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        });

        // 3.比较scrollId是否相等
        System.out.println(scrollId.equals(sameScrollId));  // true


        // 清理scroll
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        if(clearScrollResponse.isSucceeded()) {
            System.out.println(String.format("清理scrollId: %s 成功!", scrollId));
        }
    }

    // 完整scroll查询，通过循环来获取所有查询数据
    @Test
    public void test03() throws Exception {
        // 构建scroll查询
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1));
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 获取第一批次的数据结果
        SearchResponse searchResponse = client.search(searchRequest);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        // 循环获取结果，同时输出上一次的数据结果
        while (searchHits != null && searchHits.length > 0) {
            // 遍历输出结果
            for(SearchHit hit : searchHits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
            // 构建scrollRequest来获取下一批数据
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scroll = new Scroll(TimeValue.timeValueMinutes(1)); // 重新设置窗口时间，避免数据量多时获取数据的时间长而使用scrollId过期
            scrollRequest.scroll(scroll);
            searchResponse = client.searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        // 清理scroll
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        if(clearScrollResponse.isSucceeded()) {
            System.out.println(String.format("清理scrollId: %s 成功!", scrollId));
        }
    }


    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
