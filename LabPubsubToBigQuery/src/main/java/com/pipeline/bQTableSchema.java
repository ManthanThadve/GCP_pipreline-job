package com.pipeline;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;


@DefaultSchema(JavaFieldSchema.class)
public class bQTableSchema {
    int id;
    @javax.annotation.Nullable String name;
    @javax.annotation.Nullable String surname;

}
