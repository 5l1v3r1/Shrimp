package com.ttech.shrimp;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * If the value is null, replace it with the null value input, otherwise replace
 * it with the other value input,
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 *  DEFINE NVL2 com.ttech.shrimp.NVL2();
 * 
 * -- input:
 * -- (1,,2)
 * -- NVL2(string1, value_if_NOT_null, value_if_null )
 * A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
 * B = FOREACH A GENERATE NVL2(f1, f1, '-1');
 * 
 *   -- output:
 *   -- (1)
 * dump B;
 * }
 * }
 * </pre>
 * 
 * </p>
 * 
 * @author "Burak ISIKLI <burak.isikli@gmail.com>"
 */
public class NVL2 extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		try {
			if (input instanceof Tuple) {
				if (input.get(0) != null)
					return (String) input.get(1);
				else
					return (String) input.get(2);
			} else
				throw new IOException("Expected input to be Tuple, but got "
						+ input.getClass().getName());
		} catch (ExecException e) {
			throw e;
		}
	}
}