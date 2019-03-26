package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

	private static class Data implements Serializable{
		private String name = "";
		private String path = "";
		private int dist = 0;
		
		Data(String n, String p, int d){
			this.name = n;
			this.path = p;
			this.dist = d;
		}
		void setName(String n) {
			this.name = n;
		}
		void setPath(String p) {
			this.path = p;
		}
		void setDist(int d) {
			this.dist = d;
		}
		String getName() {
			return this.name;
		}
		String getPath(){
			return this.path;
		}
		int getDist() {
			return this.dist;
		}
	}
	
    private static class VProg extends AbstractFunction3<Long,Data,Data,Data> implements Serializable {
        @Override
        public Data apply(Long vertexID, Data vertexValue, Data message) {
        	if (message.getDist() == Integer.MAX_VALUE) {  // superstep 0
                return vertexValue;
            } else {                	                   // superstep > 0
            	if(vertexValue.getDist()<message.getDist()) {
            		System.out.println("Igual"+vertexValue.getName() +"-"+ vertexValue.getPath() +"-"+ vertexValue.getDist());
            		return vertexValue;
            	} else {
            		System.out.println("Cambia"+message.getName() +"-"+ message.getPath() +"-"+ message.getDist());
            		return message;
            	}
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Data,Integer>, Iterator<Tuple2<Object,Data>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Data>> apply(EdgeTriplet<Data, Integer> triplet) {
        	Tuple2<Object,Data> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Data> dstVertex = triplet.toTuple()._2();
            int dist = triplet.toTuple()._3();
            
            if(sourceVertex._2.getDist() == Integer.MAX_VALUE)
            	return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Data>(triplet.dstId(),sourceVertex._2)).iterator()).asScala();
            		
            if(sourceVertex._2.getDist() + dist < dstVertex._2.getDist()){
	            Data updatedData = new Data(dstVertex._2.getName(),sourceVertex._2.getPath()+","+dstVertex._2.getName(), sourceVertex._2.getDist()+dist);
	            ArrayList<Tuple2<Object,Data>> myarray = new ArrayList();
	            myarray.add(new Tuple2<Object,Data>(triplet.dstId(),updatedData));
	        	return JavaConverters.asScalaIteratorConverter(myarray.iterator()).asScala();
            }
            
            return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Data>>().iterator()).asScala();	
        }
    }

    private static class merge extends AbstractFunction2<Data,Data,Data> implements Serializable {
        @Override
        public Data apply(Data o, Data o2) {
        	if(o.getDist()<o2.getDist())
        		return o;
        	else 
        		return o2;
        }
    }

	public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object,Data>> vertices = Lists.newArrayList(
                new Tuple2<Object,Data>(1l, new Data("A","A",0)),
                new Tuple2<Object,Data>(2l, new Data("B","",Integer.MAX_VALUE)),
                new Tuple2<Object,Data>(3l, new Data("C","",Integer.MAX_VALUE)),
                new Tuple2<Object,Data>(4l, new Data("D","",Integer.MAX_VALUE)),
                new Tuple2<Object,Data>(5l, new Data("E","",Integer.MAX_VALUE)),
                new Tuple2<Object,Data>(6l, new Data("F","",Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Data>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Data,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Data("","",0), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Data.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Data.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        
        ops.pregel(new Data("","",Integer.MAX_VALUE),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Data.class))
            .vertices()
            .toJavaRDD()
            .foreach(v -> {
                Tuple2<Object,Data> vertex = (Tuple2<Object,Data>)v;
                System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is ["+vertex._2.getPath()+"] with cost "+vertex._2.getDist());
            });
	}

}
