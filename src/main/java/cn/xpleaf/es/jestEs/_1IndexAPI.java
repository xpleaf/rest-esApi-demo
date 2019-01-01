package cn.xpleaf.es.jestEs;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2018/12/1 12:12 AM
 *
 * Jest API使用
 * 参考：https://github.com/searchbox-io/Jest/tree/master/jest
 * 这里只演示其索引操作的功能，因为这正是es-high-level-client所缺的
 */
public class _1IndexAPI {

    // Construct a new Jest client according to configuration via factory
    JestClientFactory factory = null;

    JestClient client = null;

    @Before
    public void init() throws Exception {
        // JestClient is designed to be singleton, don't construct it for each request!
        factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                //.Builder("http://localhost:9200")
                .Builder(Arrays.asList(new String[]{"http://localhost:9200"}))  // When use collection, it must include http:// in a host
                .multiThreaded(true)
                //Per default this implementation will create no more than 2 concurrent connections per given route
                .defaultMaxTotalConnectionPerRoute(2)
                // and no more 20 connections in total
                .maxTotalConnection(10)
                .build());
        client = factory.getObject();
    }

    // 创建索引
    @Test
    public void test01() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        // 设置分片数
        settingsBuilder.put("number_of_shards", 5);
        // 设置副本数
        settingsBuilder.put("number_of_replicas", 1);
        // 索引创建对象
        CreateIndex createIndex = new CreateIndex.Builder("articles")
                .settings(settingsBuilder.build().getAsMap()).build();
        // 发送请求
        JestResult jestResult = client.execute(createIndex);
        // 查看结果
        boolean succeeded = jestResult.isSucceeded();
        System.out.println(succeeded);

    }

    /**
     * 指定mapping创建索引
     * PUT my_index
     * {
     *   "mappings": {
     *     "my_type": {
     *       "properties": {
     *         "title":{
     *           "type": "keyword"
     *         },
     *         "content":{
     *           "type": "text"
     *         }
     *       }
     *     }
     *   }
     * }
     */
    @Test
    public void test02() throws Exception {
        // 需要先创建一个索引，才能设置mapping
        client.execute(new CreateIndex.Builder("my_index")
                .settings(Settings.builder().build().getAsMap()).build());
        // 直接使用字符串json
        String myTypeProperties = "{\n" +
                "    \"my_type\": {\n" +
                "      \"properties\": {\n" +
                "        \"title\":{\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"content\":{\n" +
                "          \"type\": \"text\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }";
        // 或者使用jsonBuilder来构建json
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("my_type")
                        .startObject("properties")
                            .startObject("title")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("content")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        String myTypeProperties2 = contentBuilder.string();

        // 创建PutMapping对象
        PutMapping putMapping = new PutMapping
                .Builder("my_index", "my_type", myTypeProperties).build();

        // 发送请求
        JestResult jestResult = client.execute(putMapping);

        System.out.println(jestResult.isSucceeded());
    }

    // 判断索引是否存在
    @Test
    public void test03() throws Exception {
        IndicesExists indicesExists = new IndicesExists
                .Builder("my_index").build();
        JestResult jestResult = client.execute(indicesExists);
        System.out.println(jestResult.isSucceeded());
    }

    // 打开一个索引
    @Test
    public void test04() throws Exception {
        OpenIndex openIndex = new OpenIndex
                .Builder("my_index").build();
        JestResult jestResult = client.execute(openIndex);
        System.out.println(jestResult.isSucceeded());
    }

    // 关闭一个索引
    @Test
    public void test05() throws Exception {
        CloseIndex closeIndex = new CloseIndex
                .Builder("my_index").build();
        JestResult jestResult = client.execute(closeIndex);
        System.out.println(jestResult.isSucceeded());
    }

    // 删除一个索引
    @Test
    public void test06() throws Exception {
        DeleteIndex deleteIndex = new DeleteIndex
                .Builder("my_index").build();
        JestResult jestResult = client.execute(deleteIndex);
        System.out.println(jestResult.isSucceeded());
    }

    // 获取索引下某个type的mapping信息（schema信息）
    @Test
    public void test07() throws Exception {
        GetMapping getMapping = new GetMapping.Builder().build();
        JestResult jestResult = client.execute(getMapping);
        if(jestResult.isSucceeded()) {
            // 拿到所有索引的mapping
            JsonObject indicesJsonObject = jestResult.getJsonObject();
            // 获取my_index/my_type的mapping
            JsonObject typeJsonObject = indicesJsonObject
                    .getAsJsonObject("my_index")
                    .getAsJsonObject("mappings")
                    .getAsJsonObject("my_type")
                    .getAsJsonObject("properties");
            // 将其转换为map对象
            String mappingJson = typeJsonObject.toString();
            Map mapping = new Gson().fromJson(mappingJson, Map.class);
            System.out.println(mapping);
        }
    }

    // 获取索引列表
    @Test
    public void test08() throws Exception {
        Stats stats = new Stats.Builder().build();
        JestResult jestResult = client.execute(stats);
        if(jestResult.isSucceeded()) {
            // 拿到所有索引的元数据信息
            JsonObject jsonObject = jestResult.getJsonObject().getAsJsonObject("indices");
            // 转换为List列表
            List<String> indexList = new ArrayList<>(jsonObject.keySet());
            System.out.println(indexList);
        }
    }

    // 获取索引大小
    @Test
    public void test09() throws Exception {
        Stats stats = new Stats.Builder().build();
        JestResult jestResult = client.execute(stats);
        if(jestResult.isSucceeded()) {
            JsonObject jsonObject = jestResult.getJsonObject();
            // 总的大小，主分片+副本分片
            long indexTotalBytesSize = jsonObject
                    .getAsJsonObject("indices")
                    .getAsJsonObject("my_index")
                    .getAsJsonObject("total")
                    .getAsJsonObject("store")
                    .get("size_in_bytes").getAsLong();
            System.out.println(String.format("总的字节数：%s", indexTotalBytesSize));
            System.out.println(String.format("合计为：%s gb", indexTotalBytesSize / 1024.0 / 1024.0 / 1024.0));
            // 主分片大小，这才是真正的大小
            long indexPrimaryBytesSize = jsonObject
                    .getAsJsonObject("indices")
                    .getAsJsonObject("my_index")
                    .getAsJsonObject("primaries")
                    .getAsJsonObject("store")
                    .get("size_in_bytes").getAsLong();
            System.out.println(String.format("总的字节数：%s", indexPrimaryBytesSize));
            System.out.println(String.format("合计为：%s gb", indexPrimaryBytesSize / 1024.0 / 1024.0 / 1024.0));
        }
    }
}
