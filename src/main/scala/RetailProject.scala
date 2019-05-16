import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

object RetailProject {
  def main(args: Array[String]): Unit = {

    val databaseName="retaildatabase"
    val orderTable="et_orders_hive"
    val orderlineitemTable="et_order_lineitems_hive"
    val customerDimTable="et_customers_dim_hive"
    val customerAddDimTable="et_customer_addresses_dim_hive"
    val prodDimTable="et_products_dim_hive"
    val categoryDimTable="et_categories_dim_hive"

    val spark = SparkSession
                .builder()
                .appName("Spark-Retail Case study")
                .config("spark.master","local")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .enableHiveSupport()
                .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /*Creating database df*/
    val db_df=spark.sqlContext.sql(s"use $databaseName")


    /*Create order table df*/
    val orders_df=db_df.sqlContext.sql(s"select * from $orderTable ")

    /*Create order line item table df*/
    val orderlineitem_df=db_df.sqlContext.sql(s"select * from $orderlineitemTable ")


    //val order_lineitemCols_db =db_df.sqlContext.sql(s"select order_id,customer_id from $orderlineitemTable").count()
    val joined_df=orders_df.join(orderlineitem_df,Seq("order_id",
                                                      "customer_id",
                                                      "store_id",
                                                      "order_datetime",
                                                      "coupon_amount",
                                                      "payment_method_code",
                                                      "website_url"))
    joined_df.show(5)



    //3.Total customers--------------working!
    /*val totalValues=joined_df.agg(count("customer_id") as "Total customers",
                                  count("order_id") as "Total Orders",
                                  format_number(sum("item_price"),4)as "Total ItemPrice",
                                  format_number(sum(col("tax_amount") + col("item_price")),4) as "Price After Tax",
                                  format_number(sum("tax_amount"),4) as "Total Tax Amount" ,
                                  format_number(sum("discount_amount"),4) as "Total Discount",
                                  format_number(sum("total_paid_amount"),4) as "Total Amount",
                                  format_number(avg("total_paid_amount"),4) as "Average Sales Amount"
                                  ).show(5)*/

    /*val totalValuesperCust=joined_df.groupBy("customer_id")
                                    .agg(format_number(sum("tax_amount"),4) as "Total TaxAmount",
                                         format_number(sum("total_paid_amount"),4) as "Total Sales amount"
                                        ).show(5)*/

    //4.Date-wise Details--------working!
   /*val totalValues_bydate=joined_df.groupBy("order_datetime")
                                    .agg(count("customer_id") as "Total customers",
                                         format_number(sum("total_paid_amount"),4) as "Total Amount",
                                         format_number(sum("total_tax_amount"),4) as "Total Tax",
                                         format_number(sum(col("total_tax_amount")+col("total_paid_amount")),4) as "Total After Tax",
                                         format_number(sum(col("total_discount_amount")*col("total_paid_amount"))/sum(col("total_paid_amount"))*100,4) as "Discount Percent",
                                         format_number(avg("item_price"),4) as "Avg ItemPrice",
                                         format_number(min("item_price"),2) as "Min ItmPrice",
                                         format_number(max("item_price"),2) as "Max ItmPrice",
                                         format_number(sum(col("coupon_amount")*col("total_paid_amount"))/sum(col("total_paid_amount"))*100,2) as "Coupen/Total Perc"
                                        ).show(5)*/





    /*Create customer Dimension table df*/
    //val custDim_df=db_df.sqlContext.sql(s"select * from $customerDimTable")



    /*Join order-orderlineItemdf and CustomerDimension df*/
    //val order_Oline_custDimJoined_df=joined_df.join(custDim_df,Seq("customer_id"))
    //order_line_custDimJoined_df.show(5)
    //order_Oline_custDimJoined_df.printSchema()

    //println(custDim_df.columns.size)
    //println(joined_df.columns.size)
    //println(order_Oline_custDimJoined_df.columns.size)

    //5GenderWice total matric--------working!
   /* val genderwiseMetric=order_Oline_custDimJoined_df.groupBy("gender")
                                                      .agg(count("customer_id") as "Total customers",
                                                         count("order_id") as "Total Orders",
                                                        format_number(sum("item_price"),4) as "Total ItemPrice",
                                                        format_number(sum(col("tax_amount") + col("item_price")),4) as "Price After Tax",
                                                        format_number(sum("tax_amount"),4) as "Total Tax Amount",
                                                        format_number(sum("discount_amount"),4) as "Total Discount",
                                                        format_number(sum("total_paid_amount"),4) as "Total Amount",
                                                        format_number(avg("total_paid_amount"),4) as "Average Sales Amount"
                                                        ).show(5)
*/



    //6.Payment methodWise Metric------working!
    /*val paymentMethodMetric=order_Oline_custDimJoined_df.groupBy("payment_method_code")
                                                        .agg(count("customer_id") as "Total customers",
                                                        count("order_id") as "Total Orders",
                                                        format_number(sum("item_price"),4) as "Total ItemPrice",
                                                        format_number(sum(col("tax_amount") + col("item_price")),4) as "Price After Tax",
                                                        format_number(sum("tax_amount"),4) as "Total Tax Amount",
                                                        format_number(sum("discount_amount"),4) as "Total Discount",
                                                        format_number(sum("total_paid_amount"),4) as "Total Amount",
                                                        format_number(avg("total_paid_amount"),4) as "Average Sales Amount"
                                                        ).show(10)*/

    //7.City wise Metric---------working!
    //Create customer Dimension table df
    /*val custAddDim_df=db_df.sqlContext.sql(s"select * from $customerAddDimTable")

    val order_Oline_custAddDimJoined_df=joined_df.join(custAddDim_df,Seq("customer_id"))

    order_Oline_custAddDimJoined_df.show(10)*/

    /*val citywiseMetric=order_Oline_custAddDimJoined_df.groupBy("city")
                                                    .agg(count("customer_id") as "Total customers",
                                                    count("order_id") as "Total Orders",
                                                    format_number(sum("item_price"),4) as "Total ItemPrice",
                                                    format_number(sum(col("tax_amount") + col("item_price")),4) as "Price After Tax",
                                                    format_number(sum("tax_amount"),4) as "Total Tax Amount",
                                                    format_number(sum("discount_amount"),4) as "Total Discount",
                                                    format_number(sum("total_paid_amount"),4) as "Total Amount",
                                                    format_number(avg("total_paid_amount"),4) as "Average Sales Amount"
                                                    ).show(10)

     //8.State wise Metric------------working!
     val statewiseMetric=order_Oline_custAddDimJoined_df.groupBy("state_code")
                                                        .agg(count("customer_id") as "Total customers",
                                                          count("order_id") as "Total Orders",format_number(sum("item_price"),4) as "Total ItemPrice",
                                                          format_number(sum(col("tax_amount") + col("item_price")),4) as "Price After Tax",
                                                          format_number(sum("tax_amount"),4) as "Total Tax Amount",
                                                          format_number(sum("discount_amount"),4) as "Total Discount",
                                                          format_number(sum("total_paid_amount"),4) as "Total Amount",
                                                          format_number(avg("total_paid_amount"),4) as "Average Sales Amount"
                                                          )
    statewiseMetric.show(10)
     //9.Top10 States----------------working!

    val top10states=statewiseMetric.orderBy(col("Total customers").desc).show(10)
*/
    //10.Top 10 Postal Codes----------working!
    /*val top10postalcodes=order_Oline_custAddDimJoined_df.groupBy("zip_code").agg(count("customer_id") as "Total customers")
                                                        .orderBy(col("Total customers").desc)
                                                        .show(10)*/

    //11.
    //Create product Dimension table df
        //val prodDim_df=db_df.sqlContext.sql(s"select * from $prodDimTable")
    //prodDim_df.show(5)

    //Join Order-OrderLine item with ProductDimension
          //val order_Oline_prodDimJoined_df=joined_df.join(prodDim_df,Seq("product_id"))
    //order_Oline_prodDimJoined_df.show(5)
    //Product namewise Details----------working!
    /*val totalValues_byProdName=joined_df.groupBy("product_name")
                                        .agg(count("customer_id") as "Total customers",
                                          format_number(sum("total_paid_amount"),4) as "Total Amount",
                                          format_number(sum("total_tax_amount"),4) as "Total Tax",
                                          format_number(sum(col("total_tax_amount")+col("total_paid_amount")),4) as "Total After Tax",
                                          format_number(sum(col("total_discount_amount")*col("total_paid_amount"))/sum(col("total_paid_amount"))*100,4)
                                                        as "Discount Percent",
                                          format_number(avg("item_price"),4) as "Avg ItemPrice",
                                          format_number(min("item_price"),2) as "Min ItmPrice",
                                          format_number(max("item_price"),2) as "Max ItmPrice",
                                          format_number(sum(col("coupon_amount")*col("total_paid_amount"))/sum(col("total_paid_amount"))*100,2)
                                                        as "Coupen/Total Perc"
                                          ).show(5)*/



    //12
    //Create CategoryDimension table df
    val categoryDim_df=db_df.sqlContext.sql(s"select * from $categoryDimTable")
    //categoryDim_df.show(5)
    //categoryDim_df.agg(count("category_name") as "count").show()

    //Join Order-OrderLine-ProductDimension with CategoryDimension
      //val order_Oline_prodDim_categoryDimJoined_df = order_Oline_prodDimJoined_df.join(categoryDim_df,Seq("category_id"))
/*
    val totalValues_byCategName=order_Oline_prodDim_categoryDimJoined_df.groupBy("category_name")
                                                                        .agg(count("customer_id") as "Total customers",
                                                                            format_number(sum("total_paid_amount"),4) as "Total Amount",
                                                                            format_number(sum("total_tax_amount"),4) as "Total Tax",
                                                                            format_number(sum(col("total_tax_amount")+col("total_paid_amount")),4) as "Total After Tax",
                                                                            format_number(sum(col("total_discount_amount")*col("total_paid_amount"))/sum(col("total_paid_amount"))*100,4)
                                                                                            as "Discount Percent",
                                                                            format_number(avg("item_price"),4) as "Avg ItemPrice",
                                                                            format_number(min("item_price"),2) as "Min ItmPrice",
                                                                            format_number(max("item_price"),2) as "Max ItmPrice",
                                                                            format_number(sum(col("coupon_amount")*col("total_paid_amount"))/sum(col("total_paid_amount"))*100,2)
                                                                                            as "Coupen/Total Perc"
                                                                            ).show(5)*/

    //13.Top 10 category which shipped fast and its shipping time

   /* val top10shipped=order_Oline_prodDim_categoryDimJoined_df.groupBy("category_name")
                                                              .agg(sum(datediff(col("ship_completion_datetime"),col("ship_datetime"))*24*60*60 +
                                                                        (hour(col("ship_completion_datetime"))-hour(col("ship_datetime")))*60*60 +
                                                                        (minute(col("ship_completion_datetime"))-minute(col("ship_datetime")))*60 +
                                                                        (second(col("ship_completion_datetime"))-second(col("ship_datetime")))
                                                                      )
                                                                    as "Shipping Time(InSec)")
                                                              .orderBy(col("Shipping Time(InSec)").asc)
                                                              .show(10)*/

    //14.No:of orders returned per day
   /* val orderRetPerDay= orderlineitem_df.groupBy("item_return_datetime")
                                        .agg(count(col("item_return_datetime")) as "Return/Day")
                                        .orderBy("item_return_datetime")
                                        .show(5)*/

    //15.No:of orders refunded per day
    /*val orderRefPerDay= orderlineitem_df.groupBy("item_refund_datetime")
                                        .agg(count(col("item_refund_datetime")) as "Return/Day")
                                        .orderBy("item_refund_datetime")
                                        .show(5)*/

    //16.The percentage of coupon amount percentage per day
    val couponPercPerDay= joined_df.groupBy("order_datetime")
                                    .agg(format_number((sum(col("coupon_amount"))/sum(col("total_paid_amount")))*100,2)
                                                        as "Coupon Percentage/Day"
                                        )
                                    .orderBy("order_datetime")
                                    .show(5)

    spark.stop()

  }




}
