package com.github.fevernova.io.hdfs;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.service.checkpoint.CheckPoint;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.List;


@Getter
@ToString
@AllArgsConstructor
public class HDFSCheckPoint implements CheckPoint {


    private List<FileInfo> files;


    public HDFSCheckPoint() {

        this.files = Lists.newArrayList();
    }


    @Override public void parseFromJSON(JSONObject jsonObject) {

        JSONArray jsonArray = jsonObject.getJSONArray("files");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jo = jsonArray.getJSONObject(i);
            this.files.add(FileInfo.builder().from(jo.getString("from")).to(jo.getString("to")).build());
        }
    }
}
