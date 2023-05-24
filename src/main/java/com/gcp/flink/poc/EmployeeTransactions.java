package com.gcp.flink.poc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

public class EmployeeTransactions {

	public static void main(String[] args) throws Exception {
		// this method will return which environment flink need to run
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// it is used to read program arguments it is a hashmap
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Register these parameter as global job parameter where it will available to
		// all nodes
		env.getConfig().setGlobalJobParameters(params);
		// Employee read lines
		DataSet<String> employeeLines = env.readTextFile("gs://flink-first-poc-bucket/flink_data/employees.csv");

		// Employee Data Set
		DataSet<Tuple4<Integer, String, String, Integer>> employeeDataSet = employeeLines
				.map(new MapFunction<String, Tuple4<Integer, String, String, Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -7530120485142677984L;

					public Tuple4<Integer, String, String, Integer> map(String value) throws Exception {
						String[] data = value.split(",");

						return new Tuple4<Integer, String, String, Integer>(Integer.parseInt(data[0]), data[1], data[2],
								Integer.parseInt(data[3]));
					}
				});

		// Transaction read lines
		DataSet<String> transLines = env.readTextFile("gs://flink-first-poc-bucket/flink_data/Transactions.csv");

		// Transaction Data set
		DataSet<Tuple4<Integer, String, String, Integer>> transDataSet = transLines
				.map(new MapFunction<String, Tuple4<Integer, String, String, Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -3168375089535260627L;

					public Tuple4<Integer, String, String, Integer> map(String value) throws Exception {
						String[] data = value.split(",");

						return new Tuple4<Integer, String, String, Integer>(Integer.parseInt(data[0]), data[1], data[2],
								Integer.parseInt(data[3]));
					}
				});

		DataSet<Tuple3<Integer, String, Integer>> joinedDataSet = employeeDataSet.join(transDataSet).where(0).equalTo(0)
				.with(new JoinFunction<Tuple4<Integer, String, String, Integer>, Tuple4<Integer, String, String, Integer>, Tuple3<Integer, String, Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -2967015997114137419L;

					public Tuple3<Integer, String, Integer> join(Tuple4<Integer, String, String, Integer> first,
							Tuple4<Integer, String, String, Integer> second) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple3<Integer, String, Integer>(first.f0, second.f2, second.f3);
					}

				});
		DataSet<Tuple3<Integer, String, Integer>> result = joinedDataSet.groupBy(0, 1).aggregate(Aggregations.SUM, 2);
//		String outPutPath=EmployeeTransactions.class.getClassLoader().getResource(params.get("result")).getPath();
		if (params.has("result")) {
			
			result.map(new MapFunction<Tuple3<Integer,String,Integer>, String>() {

				public String map(Tuple3<Integer, String, Integer> value) throws Exception {
					return value.f0+","+value.f1+","+value.f2;
				}
			}).writeAsText(params.get("result"), WriteMode.OVERWRITE).setParallelism(1);
		}else {
			result.print();
		}
		env.execute("Employee Transactions Example");


	}
	

}
