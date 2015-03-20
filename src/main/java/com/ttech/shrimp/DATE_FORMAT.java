/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ttech.shrimp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * DATE_FORMAT lets you convert the date value's format which is defined by a
 * tool such as Sqoop to a long value so you can easily compare the number
 * values. It removes the special characters, white spaces and limit to 15
 * characters. If format1 is YYYY-MM-DD HH24:MI:SS, then DATE_FORMAT converts to
 * YYYYMMDDHH24MISS.
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 *  DEFINE DATE_FORMAT com.ttech.shrimp.DATE_FORMAT();
 * 
 * -- input:
 * -- (1,2015-01-13 15:36:46.0,2)
 * A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
 * B = FOREACH A GENERATE DATE_FORMAT(f2);
 * 
 *   -- output:
 *   -- (20150113153646)
 * dump B;
 * }
 * }
 * </pre>
 * 
 * </p>
 * 
 * @author "Burak ISIKLI <burak.isikli@gmail.com>"
 */
public class DATE_FORMAT extends EvalFunc<Long> {

	@Override
	public Long exec(Tuple input) throws IOException {
		try {
			String tinput = "";
			if (input == null || input.size() == 0)
				return null;
			else {
				if (input.getType(0) == DataType.CHARARRAY)
					tinput = (String) input.get(0);
				else
					throw new RuntimeException(
							"Input type expected to be chararray but got: "
									+ input.getType(0));

			}
			tinput = tinput.replaceAll("[-+.^:, ]", "");

			if (tinput.length() > 14)
				return Long.parseLong(tinput.substring(0, 14));
			else if (tinput.length() < 14)
				return Long.parseLong(String.format("%-14s", tinput).replace(
						' ', '0'));
			else
				return Long.parseLong(tinput);

		} catch (ExecException exp) {
			throw exp;
		} catch (Exception e) {
			int errCode = 2107;
			String msg = "Error while computing date_format in "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.LONG));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
	 */

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		funcList.add(new FuncSpec(DATE_FORMAT.class.getName(), s));
		return funcList;
	}
}
