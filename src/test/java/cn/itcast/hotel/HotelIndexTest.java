package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import org.apache.http.HttpHost;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelIndexTest {
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @Test
    void testInit() {
        System.out.println(client);
    }
//================对索引库的操作=================================
    @Test
    void createHotelIndex() throws IOException {
        //创建request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //准备请求的参数
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        //创建request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testExistHotelIndex() throws IOException {
        //创建request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        //发送请求
        System.out.println(client.indices().exists(request, RequestOptions.DEFAULT));
    }

//=============================对文档的操作===============================
    @Test
    void testIndexDocument() throws IOException {
        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void testGetDocumentById() throws IOException{
        GetRequest request = new GetRequest("hotel","61083");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String json = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void testUpdateDocumentById() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        request.doc(
                "name", "武汉滴水湖皇冠假日酒店",
                "city", "武汉"
        );
        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        client.delete(request, RequestOptions.DEFAULT);
    }

//=========================批量导入数据从数据库到ES====================
    @Test
    void testBulk() throws IOException {
        List<Hotel> hotels = hotelService.list();
        BulkRequest request = new BulkRequest();
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel").
                    id(hotelDoc.getId().toString()).
                    source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        client.bulk(request, RequestOptions.DEFAULT);
    }



//==========================SpringBootTest的预执行和后执行======================
    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.159.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close();
    }
}
