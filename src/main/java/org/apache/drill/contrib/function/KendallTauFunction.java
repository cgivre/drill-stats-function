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

package org.apache.drill.contrib.function;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;

@FunctionTemplate(
    name = "kendall_correlation",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.INTERNAL
)

public class KendallTauFunction implements DrillAggFunc {
  @Param
  Float8Holder xInput;

  @Param
  Float8Holder yInput;

  @Workspace
  Float8Holder prevXValue;

  @Workspace
  Float8Holder prevYValue;

  @Workspace
  IntHolder concordantPairs;

  @Workspace
  IntHolder discordantPairs;

  @Workspace
  IntHolder n;

  @Output
  Float8Holder tau;

  @Override
  public void add() {
    double xValue = xInput.value;
    double yValue = yInput.value;
    //if( xInput.isSet == 1 && yInput.isSet == 1 ){
      if (n.value > 0) {
        if ((xValue > prevXValue.value && yValue > prevYValue.value) || (xValue < prevXValue.value && yValue < prevYValue.value)) {
          concordantPairs.value = concordantPairs.value + 1;
        } else if ((xValue > prevXValue.value && yValue < prevYValue.value) || (xValue < prevXValue.value && yValue > prevYValue.value)) {
          discordantPairs.value = discordantPairs.value + 1;
        } else {
          //Tie...
        }

        prevXValue.value = xInput.value;
        prevYValue.value = yInput.value;
        n.value = n.value + 1;

      } else if(n.value == 0){
        prevXValue.value = xValue;
        prevYValue.value = yValue;
        n.value = n.value + 1;
      }

    }
  //}

  @Override
  public void setup() {
  }

  @Override
  public void reset() {
    prevXValue.value = 0;
    prevYValue.value = 0;
    concordantPairs.value = 0;
    discordantPairs.value = 0;
    n.value = 0;
  }

  @Override
  public void output() {
    double result = 0.0;
    result = (concordantPairs.value - discordantPairs.value) / (0.5 * n.value * (n.value - 1));
    tau.value = result;
  }
}
