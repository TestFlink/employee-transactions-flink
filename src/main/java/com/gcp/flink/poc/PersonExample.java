package com.gcp.flink.poc;

import java.util.Arrays;

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

public class PersonExample {

	public static void main(String[] args) throws Exception {
		// this method will return which environment flink need to run
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// it is used to read program arguments it is a hashmap
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Register these parameter as global job parameter where it will available to
		// all nodes
		env.getConfig().setGlobalJobParameters(params);
		
		DataSet<Person> dataset=env.fromCollection(Arrays.asList(new Person("Hussain",31),new Person("Supriyo",28)));
		dataset.print();
		env.execute("Person pojo Example");


	}
	

}
