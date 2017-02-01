package org.apache.drill.contrib.function;

/*
 * Copyright 2001-2004 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;

import java.util.Comparator;


@FunctionTemplate(
    name = "median",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)

public class MedianFunction implements DrillAggFunc {
    @Param
    Float8Holder in;

    @Output
    Float8Holder out;

    @Workspace
    java.util.PriorityQueue minHeap;

    @Workspace
    java.util.PriorityQueue maxHeap;

    @Workspace
    double median;

    @Override
    public void setup() {
        java.util.PriorityQueue minHeap=new java.util.PriorityQueue();
        java.util.PriorityQueue maxHeap=new java.util.PriorityQueue(11, new MyComparator());
        median = 0;
    }

    @Override
    public void add() {
        //Case one first item.
        if( minHeap.size() == 0 && maxHeap.size() == 0 ){
            median = in.value;
            minHeap.add( in.value );
        } else if( minHeap.size() == 1 && maxHeap.size() == 0 ){
            double x = in.value;
            if( x > (double)minHeap.peek()){
                maxHeap.add( x );
                median = ((double)minHeap.peek() + x ) / 2.0;
            } else {
                double y = (double)minHeap.poll();
                maxHeap.add(y);
                minHeap.add(x);
                median = ( x + y ) / 2.0;
            }
        } else {
            double x = in.value;
            if( x < median ) {
                minHeap.add(x);
            } else if ( x > median ){
                maxHeap.add(x);
            } else {
                if( minHeap.size() > maxHeap.size() ){
                    maxHeap.add(x);
                } else {
                    minHeap.add(x);
                }
            }

            int difference = maxHeap.size() - minHeap.size();
            if ( difference > 1 ) {
                double t = (double)maxHeap.poll();
                minHeap.add(t);
            } else {
                double t = (double)minHeap.poll();
                maxHeap.add(t);
            }

            if( minHeap.size() == maxHeap.size() ){
                median = ((double)minHeap.peek() + (double)maxHeap.peek()) / 2.0;
            } else {
                if( maxHeap.size() > minHeap.size() ) {
                    median = (double) maxHeap.peek();
                } else {
                    median = (double) minHeap.peek();
                }
            }
        }
    }

    @Override
    public void output() {
        out.value = median;
    }

    @Override
    public void reset() {
        minHeap.clear();
        maxHeap.clear();
        median = 0;

    }

    public class MyComparator implements Comparator<Double> {
        public int compare(Double x, Double y) {
            return (int)(y.doubleValue() - x.doubleValue());
        }
    }
}