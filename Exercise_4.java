package exercise_4;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import static org.apache.spark.sql.functions.*;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		
		// Define the path that contains the vertex txt
		String pathVertex = "src/main/resources/wiki-vertices.txt";
		
		// Create Spark RDD for the vertexes out of the txt file
		JavaRDD<String> vertexRDD = ctx.textFile(pathVertex);
		
		// Split rows in the RDD and map to a Row type JavaRDD
		JavaRDD<Row> RowVertexRDD = vertexRDD.map(line -> line.split("\t")).map(line -> RowFactory.create(line));
		
		// Create schema for the vertexes
		StructType VertexSchema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
			});
		
		// Create Spark DataFrame for the vertexes
		Dataset<Row> Vertex =  sqlCtx.createDataFrame(RowVertexRDD, VertexSchema);
		
		// Define the path that contains the vertex txt
		String pathVertexEdges = "src/main/resources/wiki-edges.txt";
		
		// Create Spark RDD for the edges out of the txt file
		JavaRDD<String> vertexEdgesRDD = ctx.textFile(pathVertexEdges);
		
		// Split rows in the RDD and map to a Row type JavaRDD
		JavaRDD<Row> RowVertexEdgesRDD = vertexEdgesRDD.map(line -> line.split("\t")).map(line -> RowFactory.create(line));
		
		// Create schema for the edges
		StructType EdgesSchema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
			});
		
		// Create Spark DataFrame for the edges
		Dataset<Row> Edges =  sqlCtx.createDataFrame(RowVertexEdgesRDD, EdgesSchema);
		
		// Create graph
		GraphFrame myGraph = GraphFrame.apply(Vertex,Edges);

		//System.out.println(myGraph);
		//myGraph.edges().show();
		//myGraph.vertices().show();
		
		// Apply PageRank algorithm to the graph
		System.out.println("Top 10 most relevant Wikipedia articles:");
		System.out.println("*****");
		
		for(double i=0.05; i<1; i+=0.05){
			System.out.println("****"+i+"****************************************");
			PageRank myPageRank = myGraph.pageRank().resetProbability(i).maxIter(5);
			myPageRank.run().vertices().orderBy(desc("pageRank")).select("id", "name", "pageRank").show(50);
		}
	    
		System.out.println("*****");
		System.out.println("PageRank successfully finished");
	}
	
}
