/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.aegisthus.mapreduce;

import com.netflix.aegisthus.io.writable.AegisthusKey;
import com.netflix.aegisthus.io.writable.AtomWritable;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumberOfReplicasReducer extends Reducer<AegisthusKey, AtomWritable, Text, Text> {
    @Override
    public void reduce(AegisthusKey key, Iterable<AtomWritable> values, Context ctx)
            throws IOException, InterruptedException {
        long count = 0;
        Column currentColumn = null;
        for (AtomWritable ignored : values) {
            OnDiskAtom atom = ignored.getAtom();
            if (atom instanceof Column) {
                Column column = (Column) atom;
                if (currentColumn == null) {
                    currentColumn = column;
                    count = 1;
                } else if (currentColumn.name().equals(column.name())) {
                    count++;
                } else {
                    Text keyOut = new Text(key.getKey().array());
                    keyOut = new Text(keyOut.toString() + "/" + new Text(currentColumn.name().array()).toString());
                    Text valueOut = new Text(Long.toString(count));
                    ctx.write(keyOut, valueOut);
                    currentColumn = column;
                    count = 1;
                }
            }
        }

        if(count != 0) {
            Text keyOut = new Text(key.getKey().array());
            keyOut = new Text(keyOut.toString() + "/" + new Text(currentColumn.name().array()).toString());
            Text valueOut = new Text(Long.toString(count));
            ctx.write(keyOut, valueOut);
        }
    }
}
