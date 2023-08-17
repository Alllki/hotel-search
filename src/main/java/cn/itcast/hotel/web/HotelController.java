package cn.itcast.hotel.web;


import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/hotel")
public class HotelController {
    @Autowired
    private IHotelService hotelService;
    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams params) {
        try {
            System.out.println(params);
            return hotelService.search(params);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
