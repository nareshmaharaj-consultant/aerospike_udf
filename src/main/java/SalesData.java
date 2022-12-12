import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;

import java.util.HashMap;
import java.util.Locale;
import java.util.Random;
import java.util.stream.IntStream;

class SalesData {

    public String segment;
    public String country;
    public String product;
    public double unitsSold;
    public double mfgPrice;
    public double salesPrice;
    public String date;

    public SalesData(String[] salesline){
        if ( salesline.length > 0 ){
            this.segment    = salesline[0];
            this.country    = salesline[1];
            this.product    = salesline[2];
            this.unitsSold  = Double.parseDouble(salesline[3]);
            this.mfgPrice   = Double.parseDouble(salesline[4]);
            this.salesPrice = Double.parseDouble(salesline[5]);
            this.date       = salesline[6];
        }
    }

//    public Operation[] getOperation(){
//        Bin segment     = new Bin("segment", Value.get( this.segment ));
//        Bin country     = new Bin("country", Value.get( this.country ));
//        Bin product     = new Bin("product", Value.get( this.product ));
//        Bin unitsSold   = new Bin("unitsSold", Value.get( this.unitsSold ));
//        Bin mfgPrice    = new Bin("mfgPrice", Value.get( this.mfgPrice ));
//        Bin salesPrice  = new Bin("salesPrice", Value.get( this.salesPrice ));
//        Bin date        = new Bin("date", Value.get( this.date ));
//
//        Operation[] operations = Operation.array(
//                Operation.put(segment),
//                Operation.put(country),
//                Operation.put(product),
//                Operation.put(unitsSold),
//                Operation.put(mfgPrice),
//                Operation.put(salesPrice),
//                Operation.put(date)
//        );
//        return operations;
//    }

    String [] segments  = {"Government", "Retail", "Health", "Sport", "Education", "Midmarket", "Channel Partners", "Enterprise", "Small Business", };
    String [] locales   = Locale.getISOCountries();
    String [] products  = {"Aerospike Real-time Data Platform","Aerospike Database","Aerospike SQL","Document Database","Features and Editions","Aerospike Tools","Kubernetes Operator","Aerospike Monitoring","Cloud","Aerospike Cloud","Aerospike on AWS","Aerospike Cloud Managed Service","Connect","Connect for Spark","Connect for Kafka","Connect for JMS","Connect for Pulsar","Connect for Presto","Connect for ESP","Technology","Real-Time Engine","Hybrid Memory Architecture","Cross Datacenter Replication","Dynamic Cluster Management","Smart Client™","Strong Consistency"};

    /*
        For aggregation queries pre-populate a map to be used as a filter as Exp are not allowed

        Country = "FRA/C":"France"
        Country/Segment = "FRA/CS":"France/Small Business"
        Country/Segment/Product = "FRA/CSP":"France/Enterprise/Hybrid Memory Architecture"

     */
    public Operation[] getOperationRandomiseData(){

        HashMap<String, String> queryFields = new HashMap<>();

        /*Segment*/
        Bin segment = new Bin("segment", Value.get( segments[new Random().nextInt( segments.length) ] ) );

        /* Country */
        Locale obj = new Locale("", locales[ new Random().nextInt( locales.length) ] );
        Bin country = new Bin("country", Value.get( obj.getDisplayCountry() ));

        /* Products */
        Bin product = new Bin("product", Value.get( products[new Random().nextInt( products.length) ] ));

        queryFields = getMaps(queryFields, country, segment, product );
        Bin queryField = new Bin("queryField", Value.get( queryFields) );

        Bin unitsSold = new Bin("unitsSold", Value.get( new Random().nextInt( (int) this.unitsSold ) ));

        Bin mfgPrice = new Bin("mfgPrice", Value.get( new Random().nextInt( (int) this.mfgPrice) ));

        Bin salesPrice = new Bin("salesPrice", Value.get( new Random().nextInt( (int) this.salesPrice) ));

        Bin date = new Bin("date", Value.get( this.date ));

        Operation[] operations = Operation.array(
                Operation.put(segment),
                Operation.put(country),
                Operation.put(product),
                Operation.put(unitsSold),
                Operation.put(mfgPrice),
                Operation.put(salesPrice),
                Operation.put(date),
                Operation.put(queryField)
        );
        return operations;
    }

    private HashMap getMaps(HashMap<String, String> queryFields, Bin country, Bin segment, Bin product) {
        String c = country.value.toString();
        String s = segment.value.toString();
        String p = product.value.toString();
        String ISO3 = convertCountryNameToIsoCode(c);

        if ( ISO3 == null ){
            // System.out.println("IS03 is null for country " + c );
            ISO3 = "None";
        }

        queryFields.put( ISO3.concat("/C"),c );
        queryFields.put( ISO3.concat("/CS"), c.concat("/").concat(s) );
        queryFields.put( ISO3.concat("/CSP"), c.concat("/").concat(s).concat("/").concat(p) );
        return queryFields;
    }

    public static String convertCountryNameToIsoCode(String countryName) {
        for (Locale l : Locale.getAvailableLocales()) {
            if (l.getDisplayCountry().equals(countryName)) {
                return l.getISO3Country();
            }
        }
        return null;
    }

}