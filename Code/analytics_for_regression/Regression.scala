import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
 // Load and parse the data 
   //val data = sc.textFile("/user/lg2681/ml/final_training.txt")
   val data = sc.textFile("/user/lg2681/ml/final_training.txt")
   // LabeledPoint is (label, [features])
   val parsedData = data.map { line =>
   val parts = line.split('\t')
   val label = parts(0).toDouble
   val features = Array(parts(1), parts(2), parts(3)) map (_.toDouble)
   LabeledPoint(label, Vectors.dense(features))
   }.cache()

  // Scale the features
  //val scaler = new StandardScaler(withMean = true, withStd = true).fit(parsedData.map(x => x.features))
  //val scaledData = parsedData.map(x => LabeledPoint(x.label,scaler.transform(Vectors.dense(x.features.toArray))))
  //val scaledData = parsedData.map(x => LabeledPoint(x.label,scaler.transform(Vectors.dense(x.features.toArray))))
  // Building the model: SGD = stochastic gradient descent
  val algorithm = new LinearRegressionWithSGD()
  algorithm.optimizer.setNumIterations(100).setStepSize(0.000001)
  // val model = LinearRegressionWithSGD.train(parsedData, 100, 0.000001);
  //val model = LinearRegressionWithSGD.train(parsedData, 100, 0.00000382);val model = algorithm.setIntercept(true).run(parsedData);
  println(s">>>> Model intercept: ${model.intercept}, weights: ${model.weights}")
  //predict one of our training data;
  println(model.predict(Vectors.dense(293.0,23.0,51.0)).toString);
  println(model.predict(Vectors.dense(701.0,28.0,21.0)).toString);
  //calculate the RMSE;
  val testdata = sc.textFile("/user/lg2681/ml/final_test.txt")

  val parsedtestData = testdata.map { line =>
      val parts = line.split('\t').map(_.toDouble)
      LabeledPoint(parts.head, Vectors.dense(parts.tail))
    }.cache()
val prediction = model.predict(parsedtestData.map(_.features));
val predictionAndTarget = prediction.zip(parsedtestData.map(_.label));
predictionAndTarget.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"));
val numTest = testdata.count()
val loss = predictionAndTarget.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numTest)
//print RMSE;
println(s"Test RMSE = $rmse.")