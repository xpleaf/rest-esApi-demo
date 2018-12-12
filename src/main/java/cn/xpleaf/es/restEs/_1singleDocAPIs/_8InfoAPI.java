package cn.xpleaf.es.restEs._1singleDocAPIs;

import org.apache.http.HttpHost;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xpleaf
 * @date 2018/12/12 5:21 PM
 *
 * 参考：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-main.html
 */
public class _8InfoAPI {

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

    // 获取es群集信息的基本测试
    @Test
    public void test01() throws Exception {
        MainResponse response = client.info();
        // 集群名称
        ClusterName clusterName = response.getClusterName();
        // 集群唯一标识
        String clusterUuid = response.getClusterUuid();
        // 执行该查询操作的节点名称
        String nodeName = response.getNodeName();
        // 执行该查询操作的节点版本
        Version version = response.getVersion();
        // 执行该查询操作的节点构建信息
        Build build = response.getBuild();

        System.out.println(String.format("clusterName: %s, clusterUuid: %s, nodeName: %s, version: %s, build: %s",
                clusterName, clusterUuid, nodeName, version, build));
    }

    @After
    public void after() throws Exception {
        restLowLevelClient.close();
    }

}
