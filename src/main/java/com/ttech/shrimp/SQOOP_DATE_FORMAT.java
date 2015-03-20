package com.ttech.shrimp;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Formating the chararray for Sqoop Loading
 * To be able to load Oracle, date format must be "DD-MM-YYYY HH24:MI:SS". 
 * SQOOP_DATE_FORMAT lets you to convert the format to be able to load to the Oracle.
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 *  DEFINE SQOOP_DATE_FORMAT com.ttech.shrimp.SQOOP_DATE_FORMAT();
 * 
 * -- input:
 * -- (20130905225900)
 * -- NVL2(string1, value_if_NOT_null, value_if_null )
 * A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
 * B = FOREACH A GENERATE SQOOP_DATE_FORMAT(f1);
 * 
 *   -- output:
 *   -- (2013-09-05 22:59:00)
 * dump B;
 * }
 * }
 * </pre>
 * 
 * </p>
 * 
 * @author "Burak ISIKLI <burak.isikli@gmail.com>"
 */
public class SQOOP_DATE_FORMAT extends EvalFunc<String> {

	public String exec(Tuple input) throws IOException {
		try {
			if (input instanceof Tuple) {
				if (input == null || input.size() < 1 || input.get(0) == null) {
					return null;
				}
				if (input.getType(0) != DataType.CHARARRAY)
					throw new ExecException(
							"Expected input's type to be chararray but got "
									+ input.getType(0));
				String strIn = DataType.toString(input.get(0));
				if (strIn.length() != 14)
					throw new ExecException(
							"Expected input's length to be 14 but got "
									+ strIn.length());
				String output = strIn.substring(0, 4) + '-'
						+ strIn.substring(4, 6) + '-' + strIn.substring(6, 8)
						+ ' ' + strIn.substring(8, 10) + ':'
						+ strIn.substring(10, 12) + ':'
						+ strIn.substring(12, 14);
				return output;
			} else
				throw new ExecException("Expected input to be Tuple, but got "
						+ input.getClass().getName());

		} catch (ExecException e) {
			throw e;
		}
	}
}