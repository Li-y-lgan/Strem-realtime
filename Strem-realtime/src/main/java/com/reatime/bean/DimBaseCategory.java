package com.reatime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @Package com.stream.domain.DimBaseCategory
 * @Author li.yan
 * @Date 2025/5/14 08:40
 * @description: base cate all data
 */

import java.io.Serializable;
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
