package cn.xpleaf.es.restEs._1singleDocAPIs;

import io.searchbox.indices.Stats;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author xpleaf
 * @date 2018/12/4 9:30 PM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-search.html
 * 另外，queryBuilder，这里只是演示基本的queryBuilder使用，更多可以参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-java-builders.html
 * 不管是查询还是聚合，只要理解了es的原理，同时query DSL没有问题，其实Java代码是非常好写的，官方文档也非常容易看懂
 */
public class _6SearchAPI_AggAPI {

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

    // 查询所有index的数据
    @Test
    public void test01() throws Exception {
        // 构建searchRequest，不指定index和type，查询所有index
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 搜索条件为匹配所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        // 处理查询结果
        // 1.先获取查询结果的基本信息
        RestStatus status = searchResponse.status();        // 状态码
        TimeValue took = searchResponse.getTook();          // 消耗时间
        boolean timedOut = searchResponse.isTimedOut();     // 是否超时
        System.out.println(String.format("status: %s, took: %s ms, timedOut: %s",
                status.getStatus(), took.getMillis(), timedOut));
        // 2.再获取查询的分片信息
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        if(failedShards > 0) {  // 如果有查询失败的分片，拿到这些分片的信息
            ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
            // TODO 查询失败分片处理策略
        }
        System.out.println(String.format("totalShards: %s, successfulShards: %s, failedShards: %s",
                totalShards, successfulShards, failedShards));
        // 3.最后获取查询的结果文档
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();   // 总的文档数
        float maxScore = hits.getMaxScore();    // 最大评分
        System.out.println(String.format("totalHits: %s, maxScore: %s", totalHits, maxScore));
        for(SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            // TODO 如果字段是nested或object，或者其它更加复杂的多层nested或object结构，则用户应该自己去进行处理
            // TODO sourceAsMap拿到key对应的value之后，再做强制类型转换就可以了
        }
    }

    /**
     * 分页查询，同时指定index和type，指定source，其实就是构建下面的查询：
     * {
     *   "from" : 0,
     *   "size" : 2,
     *   "timeout" : "1m",
     *   "query" : {
     *     "match_all" : {
     *       "boost" : 1.0
     *     }
     *   },
     *   "_source" : {
     *     "includes" : [
     *       "title",
     *       "user",
     *       "message"
     *     ],
     *     "excludes" : [ ]
     *   },
     *   "sort" : [
     *     {
     *       "_score" : {
     *         "order" : "desc"
     *       }
     *     },
     *     {
     *       "_uid" : {
     *         "order" : "asc"
     *       }
     *     }
     *   ]
     * }
     * @throws Exception
     */
    @Test
    public void test02() throws Exception {
        SearchRequest searchRequest = new SearchRequest("posts").types("doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 搜索条件为匹配所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 设置分页条件
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        // 设置超时时间
        searchSourceBuilder.timeout(TimeValue.timeValueMinutes(1));
        // 设置需要获取的字段
        String[] includes = new String[]{"title", "user", "message"};
        String[] excludes = Strings.EMPTY_ARRAY;
        searchSourceBuilder.fetchSource(true);  // 默认为true，设置为false就不获取文档内容
        searchSourceBuilder.fetchSource(includes, excludes);
        // 设置排序规则
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));     // 通过_score降序排序，默认
        searchSourceBuilder.sort(new FieldSortBuilder("_uid").order(SortOrder.ASC));    // 同时通过_id升序排序，如果_score相同
        // 注意，通过_id进行排序，上面写的确实就是_uid，而不是_id，如果直接写_id，会报错

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        // 直接获取查询结果
        SearchHits hits = searchResponse.getHits();
        for(SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 基本桶聚合操作
     * 下面只演示基本的聚合操作，并且没有做前置过滤，实际应用中，可以先执行前置过滤，再进行聚合
     * 也就是说，对于searchSourceBuilder，可以同时设置query条件和aggregation条件
     */
    @Test
    public void test03() throws Exception {
        SearchRequest searchRequest = new SearchRequest("tvs").types("sales");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建聚合条件
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders
                .terms("group_by_brand").field("brand").minDocCount(1);
        searchSourceBuilder.aggregation(termsAggregationBuilder).size(0);   // 不需要返回source，size设置为0
        searchRequest.source(searchSourceBuilder);
        // 聚合查询
        SearchResponse searchResponse = client.search(searchRequest);
        // 获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> resultMap = aggregations.getAsMap();
        Aggregation brandAgg = resultMap.get("group_by_brand");
        // 转换为Terms聚合结果
        Terms termsBrandAgg = (Terms) brandAgg;
        if(termsBrandAgg.getBuckets().size() > 0) {
            for(Terms.Bucket bucket : termsBrandAgg.getBuckets()) {
                Object bucketKey = bucket.getKey();
                long docCount = bucket.getDocCount();
                System.out.println(String.format("bucket: %s, docCount: %s", bucketKey, docCount));
            }
        }
    }

    // 执行桶聚合和指标聚合
    @Test
    public void test04() throws Exception {
        SearchRequest searchRequest = new SearchRequest("tvs").types("sales");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建聚合条件
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders
                .terms("group_by_brand").field("brand").minDocCount(1);
        // 构建指标聚合stats，其包含了min max sum avg属性
        termsAggregationBuilder.subAggregation(AggregationBuilders.stats("stats_price").field("price"));
        // 再构建一个指标聚合avg
        termsAggregationBuilder.subAggregation(AggregationBuilders.avg("avg_price").field("price"));
        searchSourceBuilder.aggregation(termsAggregationBuilder).size(0);   // 不需要返回source，size设置为0
        searchRequest.source(searchSourceBuilder);
        // 聚合查询
        SearchResponse searchResponse = client.search(searchRequest);
        // 获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> resultMap = aggregations.getAsMap();
        Aggregation brandAgg = resultMap.get("group_by_brand");
        // 转换为Terms聚合结果
        Terms termsBrandAgg = (Terms) brandAgg;
        if(termsBrandAgg.getBuckets().size() > 0) {
            for(Terms.Bucket bucket : termsBrandAgg.getBuckets()) {
                Object bucketKey = bucket.getKey();
                long docCount = bucket.getDocCount();
                System.out.println(String.format("bucket: %s, docCount: %s", bucketKey, docCount));
                // 获取指标聚合的结果-stats_price
                Aggregation statsPrice = bucket.getAggregations().get("stats_price");
                // 转换类型
                ParsedStats parsedStatsPrice = (ParsedStats) statsPrice;
                System.out.println(String.format("bucket: %s, stats: {min: %s, max: %s, sum: %s, avg: %s}",
                        bucketKey, parsedStatsPrice.getMin(), parsedStatsPrice.getMax(), parsedStatsPrice.getSum(), parsedStatsPrice.getAvg()));
                // 获取指标聚合的结果-avg_price
                ParsedAvg parsedAvgPrice = (ParsedAvg)bucket.getAggregations().get("avg_price");
                System.out.println(String.format("bucket: %s, avg: %s", bucketKey, parsedAvgPrice.getValue()));
            }
        }
    }


    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
