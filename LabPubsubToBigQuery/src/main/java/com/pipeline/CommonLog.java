package com.pipeline;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import javax.annotation.Nullable;

@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    int id;
    @javax.annotation.Nullable
    String name;
    @javax.annotation.Nullable
    String surname;

    public CommonLog() {
        this.id = 0;
        this.name = "default-name";
        this.surname = "default-surname";
    }
}
