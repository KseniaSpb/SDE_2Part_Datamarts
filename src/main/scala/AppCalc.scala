
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, avg, col, collect_list, concat, concat_ws, count, countDistinct, date_format, dayofmonth, from_unixtime, get_json_object, hour, lit, max, minute, month, round, sum, to_timestamp, to_utc_timestamp, unix_timestamp, year}

object AppCalc extends App {

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("yarn")
    .config("spark.sql.warehouse.dir", "№№№№№№№№")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val results = "school_de.results_№№№№№№№№"
  val tickets = spark.table("school_de.bookings_tickets")
  val ticket_flights = spark.table("school_de.bookings_ticket_flights")
  val flights_v = spark.table("school_de.bookings_flights_v")
  val airports = spark.table("school_de.bookings_airports")
  val routes = spark.table("school_de.bookings_routes")
  val aircrafts = spark.table("school_de.bookings_aircrafts")

  // 1 Вывести максимальное количество человек в одном бронировании

  val q1_1 = tickets
    .groupBy("book_ref")
    .agg(count("passenger_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)

  val q1_ans = q1_1.select(lit(1).as("id"),col("cnt").cast("string").as("response"))

  //  2 Вывести количество бронирований с количеством людей больше среднего значения
  //  людей на одно бронирование

  val q2_sum = tickets.count()
  val q2_count = tickets.distinct().count()
  val q2_avg = q2_sum/q2_count

  val q2_1 = tickets
    .groupBy("book_ref")
    .agg(count("book_ref").as("cnt"))

  val q2_2 = q2_1.where(q2_1("cnt") > q2_avg).agg(count("book_ref").as("cnt2"))

  val q2_ans = q2_2.select(lit(2).as("id"),col("cnt2").cast("string").as("response"))

  //  3 Вывести количество бронирований, у которых состав пассажиров повторялся два и более раза,
  //  среди бронирований с максимальным количеством людей (п.1)?

  val q3_max = tickets
    .groupBy("book_ref")
    .agg(count("passenger_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)
    .collect()(0)(1)

  val q3_1 = tickets
    .groupBy("book_ref")
    .agg(count("passenger_id").as("cnt"))
    .filter(col("cnt") === q3_max)
    .select(col("book_ref").as("br"))

  val q3_2 = q3_1
    .join(tickets, q3_1("br") === tickets("book_ref"), "inner")
    .orderBy("br", "passenger_id")
    .groupBy("br")
    .agg(concat_ws(",", collect_list("passenger_id")).alias("pass_in_br"))
    .groupBy("pass_in_br")
    .agg(count("pass_in_br").as("cnt2"))
    .filter(col("cnt2") > 2)
    .agg(count("cnt2").as("cnt3"))
    .select(lit(3).as("id"),col("cnt3").cast("string").as("response"))

  val q3_ans = q3_2

  //  4 Вывести номера брони и контактную информацию по пассажирам в брони (passenger_id, passenger_name, contact_data)
  //  с количеством людей в брони = 3

  val q4_1 = tickets
    .groupBy("book_ref")
    .agg(count("passenger_id").as("cnt"))
    .filter(col("cnt") === 3)
    .select(col("book_ref").as("br"))

  val q4_2 = q4_1
    .join(tickets, q4_1("br") === tickets("book_ref"), "inner")
    .orderBy("br", "passenger_id", "passenger_name", "contact_data")
    .select(concat(col("br"), lit("|"), col("passenger_id"), lit("|"), col("passenger_name"), lit("|"), col("contact_data")).as("conc"))
    .orderBy("conc")

  val q4_ans = q4_2
    .select(lit(4).as("id"),col("conc").cast("string").as("response"))
    .orderBy("response")

  //  5 Вывести максимальное количество перелётов на бронь

  val q5_1 = ticket_flights.select(col("ticket_no").as("tf_tn"), col("flight_id"))
  val q5_2 = tickets.select(col("ticket_no").as("t_tn"), col("book_ref"))

  val q5_3 = q5_1
    .join(q5_2, q5_1("tf_tn") === q5_2("t_tn"))
    .groupBy("book_ref")
    .agg(count("flight_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)

  val q5_ans = q5_3
    .select(lit(5).as("id"),col("cnt").cast("string").as("response"))

  //  6 Вывести максимальное количество перелётов на пассажира в одной брони

  val q6_1 = ticket_flights.select(col("ticket_no").as("tf_tn"), col("flight_id"))
  val q6_2 = tickets.select(col("ticket_no").as("t_tn"), col("book_ref"), col("passenger_id"))

  val q6_3 = q6_1
    .join(q6_2, q6_1("tf_tn") === q6_2("t_tn"))
    .groupBy("book_ref", "passenger_id")
    .agg(count("flight_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)

  val q6_ans =  q6_3
    .select(lit(6).as("id"),col("cnt").cast("string").as("response"))

  //  7 Вывести максимальное количество перелётов на пассажира

  val q7_1 = ticket_flights.select(col("ticket_no").as("tf_tn"), col("flight_id"))
  val q7_2 = tickets.select(col("ticket_no").as("t_tn"), col("passenger_id"))

  val q7_3 = q7_1
    .join(q7_2, q7_1("tf_tn") === q7_2("t_tn"))
    .groupBy("passenger_id")
    .agg(count("flight_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)

  val q7_ans = q7_3
    .select(lit(7).as("id"),col("cnt").cast("string").as("response"))

    8 Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name, contact_data) и общие траты
    на билеты, для пассажира потратившему минимальное количество денег на перелеты

  val q8_1 = ticket_flights.select(col("ticket_no").as("tf_tn"), col("flight_id").as("fl_id"), col("amount"))
  val q8_2 = tickets.select(col("ticket_no").as("t_tn"), col("passenger_id").as("pass_id"), col("passenger_name"), col("contact_data"))
  val q8_5 = flights_v.select(col("status"), col("flight_id").as("fl_id_v")).filter(col("status") =!= "Cancelled")

  val q8_min = q8_1
    .join(q8_2, q8_1("tf_tn") === q8_2("t_tn"))
    .join(q8_5, q8_1("fl_id") === q8_5("fl_id_v"))
    .groupBy("pass_id")
    .agg(sum("amount").as("sum_amount"))
    .orderBy(col("sum_amount").asc)
    .limit(1)
    .collect()(0)(1)

  val q8_3 = q8_2
    .join(q8_1, q8_1("tf_tn") === q8_2("t_tn"))
    .join(q8_5, q8_1("fl_id") === q8_5("fl_id_v"))
    .groupBy("pass_id")
    .agg(sum("amount").as("sum_amount"))
    .filter(col("sum_amount") === q8_min)

  val q8_4 = q8_3
    .join(tickets, tickets("passenger_id") === q8_3("pass_id"))
    .orderBy("pass_id", "passenger_name", "contact_data")
    .select(concat(col("pass_id"), lit("|"), col("passenger_name"), lit("|"), col("contact_data"), lit("|"), col("sum_amount")).as("conc"))
    .orderBy("conc")

  val q8_ans = q8_4
    .select(lit(8).as("id"),col("conc").cast("string").as("response"))

  //  9 Вывести контактную информацию по пассажиру(ам) (passenger_id, passenger_name, contact_data) и общее время
  //  в полётах, для пассажира, который провёл максимальное время в полётах

  val q9_1 = ticket_flights.select(col("ticket_no").as("tf_tn"), col("amount"), col("flight_id").as("fl_id"))
  val q9_2 = tickets.select(col("ticket_no").as("t_tn"), col("passenger_id").as("pass_id"), col("passenger_name"), col("contact_data"))
  val q9_3 = flights_v.select(col("status"),(to_timestamp(col("actual_duration")).as("tmp")), col("flight_id").as("fl_id_v")).where(col("tmp").isNotNull).where(col("status") === "Arrived")
    .withColumn("hours", hour(col("tmp")).cast("int"))
    .withColumn("mins", minute(col("tmp")).cast("int"))
    .withColumn("all_min", col("hours") * 60 + col("mins"))

  val q9_max = q9_1
    .join(q9_2, q9_1("tf_tn") === q9_2("t_tn"))
    .join(q9_3, q9_1("fl_id") === q9_3("fl_id_v"))
    .groupBy("pass_id")
    .agg(sum("all_min").as("sum_all"))
    .orderBy(col("sum_all").desc)
    .limit(1)
    .collect()(0)(1)

  val q9_4 = q9_1
    .join(q9_2, q9_1("tf_tn") === q9_2("t_tn"))
    .join(q9_3, q9_1("fl_id") === q9_3("fl_id_v"))
    .groupBy("pass_id")
    .agg(sum("all_min").as("sum_all"))
    .filter(col("sum_all") === q9_max)
    .withColumn("hours2", (col("sum_all")/60).cast("int"))
    .withColumn("mins2", col("sum_all") - (col("hours2") * 60))
    .withColumn("convert",concat(col("hours2"), lit(":"), col("mins2"), lit(":00")))

  val q9_5 = q9_4
    .join(tickets, tickets("passenger_id") === q9_4("pass_id"))
    .orderBy("pass_id", "passenger_name", "contact_data")
    .select(concat(col("pass_id"), lit("|"), col("passenger_name"), lit("|"), col("contact_data"), lit("|"), col("convert")).as("conc"))
    .orderBy("conc")

  val q9_ans = q9_5
    .select(lit(9).as("id"),col("conc").cast("string").as("response"))

  //  10 Вывести город(а) с количеством аэропортов больше одного

  val q10_1 = airports
    .groupBy("city")
    .agg(count("city").as("cnt"))
    .filter(col("cnt") > 1)
    .orderBy(col("city").asc)

  val q10_ans = q10_1
    .select(lit(10).as("id"),col("city").cast("string").as("response"))

  //   11 Вывести город(а), у которого самое меньшее количество городов прямого сообщения

  val q11_min = routes
    .groupBy("departure_city")
    .agg(countDistinct("arrival_city").as("cnt"))
    .orderBy(col("cnt").asc)
    .limit(1)
    .collect()(0)(1)

  val q11_2 = routes
    .groupBy("departure_city")
    .agg(countDistinct("arrival_city").as("cnt"))
    .filter(col("cnt") === q11_min)
    .orderBy(col("departure_city").asc)

  val q11_ans = q11_2
    .select(lit(11).as("id"),col("departure_city").cast("string").as("response"))

  //  13 Вывести города, до которых нельзя добраться без пересадок из Москвы?

  val q13_1 = airports.select(col("city").as("city2")).where(col("city2") =!= "Москва").distinct()
  val q13_2 = flights_v.where(col("departure_city") === "Москва")
    .select(col("arrival_city").as("city2"))

  val q13_3 = q13_1.except(q13_2).distinct().orderBy("city2")

  val q13_ans = q13_3
    .select(lit(13).as("id"),col("city2").cast("string").as("response"))

  //  14 Вывести модель самолета, который выполнил больше всего рейсов

  val q14_1 = flights_v.select(col("aircraft_code").as("f_code"), col("flight_id")).where(col("status") === "Arrived")
  val q14_2 = aircrafts.select(col("aircraft_code").as("a_code"), col("model"))

  val q14_3 = q14_1
    .join(q14_2, q14_2("a_code") === q14_1("f_code"))
    .groupBy("model")
    .agg(count("flight_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)

  val q14_ans = q14_3
    .select(lit(14).as("id"),col("model").cast("string").as("response"))

  //  15 Вывести модель самолета, который перевез больше всего пассажиров

  val q15_1 = flights_v.select(col("aircraft_code").as("f_code"), col("flight_id").as("fl_id_v")).where(col("status") === "Arrived")
  val q15_2 = aircrafts.select(col("aircraft_code").as("a_code"), col("model"))
  val q15_3 = ticket_flights.select(col("ticket_no").as("tk_n_f"), col("flight_id").as("fl_id"))
  val q15_4 = tickets.select(col("ticket_no").as("tk_n"), col("passenger_id"))

  val q15_5 = q15_1
    .join(q15_2, q15_2("a_code") === q15_1("f_code"))
    .join(q15_3, q15_3("fl_id") === q15_1("fl_id_v"))
    .join(q15_4, q15_4("tk_n") === q15_3("tk_n_f"))
    .groupBy("model")
    .agg(count("passenger_id").as("cnt"))
    .orderBy(col("cnt").desc)
    .limit(1)

  val q15_ans = q15_5
    .select(lit(15).as("id"),col("model").cast("string").as("response"))

  //  16 Вывести отклонение в минутах суммы запланированного времени перелета от фактического по всем перелётам

  val q16_1 = flights_v
    .select(col("status"),(to_timestamp(col("scheduled_duration")).as("tmp_shed")), (to_timestamp(col("actual_duration")).as("tmp_act")))
    .where(col("tmp_shed").isNotNull)
    .where(col("tmp_act").isNotNull)
    .where(col("status") === "Arrived")
    .withColumn("shed", unix_timestamp(col("tmp_shed")))
    .withColumn("act", unix_timestamp(col("tmp_act")))
    .select(((abs(sum(col("shed")) - sum(col("act"))))/60).cast("int").as("diff"))

  val q16_ans = q16_1
    .select(lit(16).as("id"),col("diff").cast("string").as("response"))

  //  17 Вывести города, в которые осуществлялся перелёт из Санкт-Петербурга 2016-09-13

  val q17_1 = flights_v
    .select(col("arrival_city"), (to_timestamp(col("actual_departure")).as("date")), col("departure_city"), col("status"))
    .where(col("actual_departure").isNotNull)
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .where(col("year") === "2016" && col("month") === "09" && col("day") === "13" && col("departure_city") === "Санкт-Петербург")
    .filter(col("status") === "Arrived" || col("status") === "Departed")
    .orderBy(col("arrival_city").asc)
    .select(col("arrival_city"))
    .distinct()

  val q17_ans = q17_1
    .select(lit(17).as("id"),col("arrival_city").cast("string").as("response"))

  //  18 Вывести перелёт(ы) с максимальной стоимостью всех билетов

  val q18_max = ticket_flights
    .select(col("flight_id"), col("amount"))
    .groupBy("flight_id")
    .agg(sum("amount").as("summ_am"))
    .orderBy(col("summ_am").desc)
    .limit(1)
    .collect()(0)(1)

  val q18_ans =
    ticket_flights
      .select(col("flight_id"), col("amount"))
      .groupBy("flight_id")
      .agg(sum("amount").as("summ_am"))
      .where(col("summ_am") === q18_max)
      .select(lit(18).as("id"),col("flight_id").cast("string").as("response"))

  //  19 Выбрать дни в которых было осуществлено минимальное количество перелётов

  val q19_ans = flights_v
    .select(col("actual_departure"), to_timestamp(col("actual_departure")).as("date"), col("flight_id"))
    .where(col("status") =!= "Cancelled" and col("actual_departure").isNotNull)
    .withColumn("date2", date_format(col("date"), "yyyy-MM-dd"))
    .groupBy("date2")
    .agg(count("flight_id").as("cnt"))
    .orderBy(col("cnt").asc)
    .limit(1)
    .select(lit(19).as("id"),col("date2").cast("string").as("response"))

  //  20 Вывести среднее количество вылетов в день из Москвы за 09 месяц 2016 года

  val q20_ans = flights_v
    .select(col("departure_city"), col("flight_id"), to_timestamp(col("actual_departure")).as("date"), col("status"))
    .where(col("departure_city") === "Москва")
    .where(col("status") === "Arrived" || col("status") === "Departed")
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .where(col("year") === "2016" && col("month") === "09")
    .agg((count("flight_id")/30).cast("int").as("cnt"))
    .select(lit(20).as("id"),col("cnt").cast("string").as("response"))

  //  21 Вывести топ 5 городов у которых среднее время перелета до пункта назначения больше 3 часов

  val q21_ans = flights_v
    .select(col("departure_city"), to_timestamp(col("actual_duration")).as("date"))
    .where(col("actual_duration").isNotNull)
    .withColumn("hour", col("date").cast("int"))
    .withColumn("min", col("date").cast("int"))
    .withColumn("all_min", col("hour") * 60 + col("min"))
    .groupBy("departure_city")
    .agg((avg("all_min")/60).as("cnt"))
    .filter(col("cnt") > 3)
    .orderBy(col("cnt").desc)
    .limit(5)
    .orderBy(col("departure_city").asc)
    .select(lit(21).as("id"),col("departure_city").cast("string").as("response"))

  val ans_DF =  q1_ans
    .union(q2_ans)
    .union(q3_ans)
    .union(q4_ans)
    .union(q5_ans)
    .union(q6_ans)
    .union(q7_ans)
    .union(q8_ans)
    .union(q9_ans)
    .union(q10_ans)
    .union(q11_ans)
    .union(q12_ans)
    .union(q13_ans)
    .union(q14_ans)
    .union(q15_ans)
    .union(q16_ans)
    .union(q17_ans)
    .union(q18_ans)
    .union(q19_ans)
    .union(q20_ans)
    .union(q21_ans)

    .write
    .mode("overwrite")
    .saveAsTable(results)

}