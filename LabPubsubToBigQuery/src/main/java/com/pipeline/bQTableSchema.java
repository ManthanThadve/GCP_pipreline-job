package com.pipeline;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
public class bQTableSchema {
    int id;
    String name;
    String surname;

    @SchemaCreate
    public bQTableSchema(int id,String name,String surname) {
     this.id=id;
     this.name=name;
     this.surname=surname;
    }

}
