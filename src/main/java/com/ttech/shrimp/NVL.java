package com.ttech.shrimp;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * NVL lets you replace null (returned as a blank) with a string in the results of a query. 
 * If expr1 is null, then NVL returns expr2. If expr1 is not null, then NVL returns expr1.
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 *  DEFINE NVL com.ttech.shrimp.NVL();
 * 
 * -- input:
 * -- (1,,2)
 * A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
 * B = FOREACH A GENERATE NVL(f1, '-1') as f1, NVL(f2, '-1') as f2, NVL(f3, '-1') as f3;
 * 
 *   -- output:
 *   -- (1, -1, 2)
 * dump B;
 * }
 * }
 * </pre>
 * 
 * </p>
 * 
 * @author "Burak ISIKLI <burak.isikli@gmail.com>"
 */
public class NVL extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		try {
			if (input instanceof Tuple) {
				String result = (String) input.get(1);
				if (input.get(0) == null)
					return result;
				else {
					// String output = (String) input.get(0);
					// return output;

					if (input.getType(0) == DataType.INTEGER)
						return Integer.toString((Integer) input.get(0));
					else if (input.getType(0) == DataType.CHARARRAY)
						return (String) input.get(0);
					else if (input.getType(0) == DataType.LONG)
						return Long.toString((Long) input.get(0));
					else if (input.getType(0) == DataType.FLOAT)
						return Float.toString((Float) input.get(0));
					else if (input.getType(0) == DataType.DOUBLE)
						return Double.toString((Double) input.get(0));
					else
						throw new RuntimeException("Input type error");

				}

			} else
				throw new IOException("Expected input to be Tuple, but got "
						+ input.getClass().getName());
		} catch (ExecException e) {
			throw e;
		}
	}
}