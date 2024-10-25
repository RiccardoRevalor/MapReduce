package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                MonthClass,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<MonthClass> values, // Input value type
        Context context) throws IOException, InterruptedException {
         
        //strategy: for each year you have an hashmap with the total income for each month
        HashMap<String, Double> IncomeByMonth = new HashMap<String, Double>();

        Double yearlyIncome = 0.0;

        //assign all the incomes to every month in the hashmap

        for (MonthClass value : values) {
            // Retrieve the current income for the current month
            Double income = IncomeByMonth.get(value.getMonthID());

            yearlyIncome += income;

            if (income != null) {
                // This month is already in the hashmap (other local incomes for this month have been already analyzed).
                // Update the total income for this month
                IncomeByMonth.put(new String(value.getMonthID()), new Double(value.getIncome() + income));
            } else {
                // First occurrence of this monthId
                // Insert monthid - income in the hashmap
                IncomeByMonth.put(new String(value.getMonthID()), new Double(value.getIncome()));
            }
        }


        //emit first the monthly income for each entry
        for (Entry<String, Double> entry : IncomeByMonth.entrySet()) {
            context.write(new Text(key.toString() + "-" + entry.getKey()), new DoubleWritable(entry.getValue()));
        }

        //emit the yearly income
        context.write(new Text(key.toString() + "-total"), new DoubleWritable(yearlyIncome));
        
    }
}
