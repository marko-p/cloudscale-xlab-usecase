package si.xlab.gaea.geoserver.wps;

/**
 * Gaea+, Open Source Java GIS client with advanced 3D visualization
 * http://www.gaeaplus.eu
 * https://github.com/gaeaplus
 * <p/>
 * (C) 2015, XLAB d.o.o.
 * <p/>
 * This process is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 */


import com.vividsolutions.jts.geom.Point;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.vector.AggregateProcess;
import org.geotools.process.vector.AggregateProcess.AggregationFunction;
import org.geotools.process.vector.AggregateProcess.Results;
import org.geotools.process.vector.SimpleProcessingCollection;
import org.geotools.util.NullProgressListener;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.util.ProgressListener;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * A process providing a "group by" functionality on specified attributes and aggregates on another set of attributes
 *
 * @author Marko Praprotnik - XLAB
 */
@DescribeProcess(title = "GroupBy", description = "Groups features on specified attributes and aggregates on another set of attributes.")
public class GroupByProcess implements GeoServerProcess
{
    private static final Logger LOG = Logger.getLogger(GroupByProcess.class.getName());
//  Destinacija logov: /var/lib/tomcat7/logs/catalina.out

    // the functions this process can handle
//    public enum GroupingFunction
//    {
//        Count, Average, Max, Median, Min, StdDev, Sum
//    }

    //    public SimpleFeatureCollection execute (  // Ko bo pripravljen delegat

    @DescribeResult(name = "result", description = "Grouping results")
    public SimpleFeatureCollection execute(
            @DescribeParameter(name = "features", description = "Input feature collection") SimpleFeatureCollection features,
            @DescribeParameter(name = "groupingAttributes", min = 1, description = "A list of attributes on which to perform grouping, delimited by commas.") String groupingAttributes,
            @DescribeParameter(name = "aggregationSpecification", min = 0, description = "The aggregation specification of the form aggregation(attribute), delimited by commas.") String aggregationSpecification
    ) throws ProcessException, IOException
    {
        // TODO: to je samo za debugging, odstrani ko bos konec
//        String rv = "";

        ArrayList<String> groupAttrs = null;
//        ArrayList<String> aggrSpec = null;
//        ArrayList<String> unknownFoos;
//        ArrayList<String> unknownAttrs;
//        ArrayList<String> unknownAggrs;
        List<AttributeDescriptor> allAttrs;
//        HashMap<String, List<AggregationFunction>> aggrFunctions;
//        HashSet<Integer> groupIndexes;
//        HashSet<Integer> aggIndexes;
//        HashMap<String, AttributeDescriptor> groupDescriptors;
//        HashMap<String, AttributeDescriptor> aggrDescriptors;

        // Check if mandatory parameters are not null
        if (features == null) {
            throw new ProcessException("Please supply a feature collection");
        }
        if (groupingAttributes == null) {
            throw new ProcessException("Please specify the grouping attribute(s)");
        } else {
            groupAttrs = new ArrayList<String>(Arrays.asList(groupingAttributes.split(", ")));
        }

        // Check if the feature collection has more attributes than the grouping attributes list
        allAttrs = features.getSchema().getAttributeDescriptors();
        if (groupAttrs.size() > allAttrs.size()) {
            throw new ProcessException("The count of supplied grouping attributes is larger than the count of all attributes in the feature collection");
        }

//        // Check if aggregation functions are valid,
        // Build a map of aggregation attributes and functions to run on them
//        aggrFunctions = getAggregationFunctionsMap(aggregationSpecification);
//        unknownFoos = new ArrayList<String>();
//        if (aggregationSpecification != null) {
//            aggrSpec = new ArrayList<String>(Arrays.asList(aggregationSpecification.split(", ")));
//            for (String aggregation : aggrSpec) {
//                String[] aggSplit = aggregation.replace(")", "").split("\\(", 2);
//
//                // We only check aggregation functions
//                boolean ok = false;
//                for (AggregationFunction foo : AggregationFunction.values()) {
//                    if (aggSplit[0].equalsIgnoreCase(foo.name())) {
//                        ok = true;
//                        List<AggregationFunction> foos;
//                        if (aggrFunctions.containsKey(aggSplit[1])) {
//                            foos = aggrFunctions.get(aggSplit[1]);
//                        } else {
//                            foos = new ArrayList<AggregationFunction>();
//                        }
//                        foos.add(foo);
//                        aggrFunctions.put(aggSplit[1], foos);
//                        break;
//                    }
//                }
//
//                if (!ok) {
//                    unknownFoos.add(aggSplit[0]);
//                }
//            }
//        }

        // Collect grouping and aggregation attribute indexes and check for their validity
//        groupIndexes = new HashSet<Integer>();
//        aggIndexes = new HashSet<Integer>();
//        groupDescriptors = new HashMap<String, AttributeDescriptor>();
//        aggrDescriptors = new HashMap<String, AttributeDescriptor>();
//        unknownAttrs = new ArrayList<String>(groupAttrs);
//        unknownAggrs = new ArrayList<String>(aggrFunctions.keySet());
//        for (int i = 0; i < allAttrs.size(); i++) {
//            String attName = allAttrs.get(i).getLocalName();
//
//            // Collect grouping attribute indexes
//            for (String groupAttName : unknownAttrs) {
//                if (attName.equals(groupAttName)) {
//                    groupIndexes.add(i);
//                    groupDescriptors.put(attName, allAttrs.get(i));
//                    unknownAttrs.remove(groupAttName);
//                    break;
//                }
//            }
//
//            // Check aggregation attributes for validity
//            for (String aggAttName : unknownAggrs) {
//                if (attName.equals(aggAttName)) {
//                    aggIndexes.add(i);
//                    aggrDescriptors.put(attName, allAttrs.get(i));
//                    unknownAggrs.remove(aggAttName);
//                    break;
//                }
//            }
//
//            if (unknownAttrs.isEmpty() && unknownAggrs.isEmpty()) {
//                break;
//            }
//        }
//
//        // Throw exception if we found unknown grouping attributes
//        if (!unknownAttrs.isEmpty()) {
//            ArrayList<String> validAttrNames = new ArrayList<String>();
//            for (AttributeDescriptor desc : allAttrs) {
//                validAttrNames.add(desc.getLocalName());
//            }
//            String msg = makeExceptionString("Could not find grouping attribute(s) ", unknownAttrs, ". The valid values are ", validAttrNames);
//
//            rv = rv.concat(", ").concat(msg);
////            throw new ProcessException(msg);
//        }
//
//        // Throw exception if we found unknown aggregation attributes
//        if (!unknownAggrs.isEmpty()) {
//            ArrayList<String> validAttrNames = new ArrayList<String>();
//            for (AttributeDescriptor desc : allAttrs) {
//                validAttrNames.add(desc.getLocalName());
//            }
//            String msg = makeExceptionString("Could not find aggregation attribute(s) ", unknownAggrs, ". The valid values are ", validAttrNames);
//
//            rv = rv.concat(", ").concat(msg);
////            throw new ProcessException(msg);
//        }

        // Split features into FeatureCollections, based on supplied grouping attribute names
//        HashMap<List<Object>, DefaultFeatureCollection> featureMap = splitFeatures(features, groupAttrs);
//
//        // Aggregate on each split FeatureCollection
//        AggregateProcess aggregateProcess = new AggregateProcess();
//        for (List<Object> key : featureMap.keySet()) {
//            DefaultFeatureCollection fc = featureMap.get(key);
//            for (String agg : aggrFunctions.keySet()) {
//                Results results = aggregateProcess.execute(fc, agg, new HashSet<AggregationFunction>(aggrFunctions.get(agg)), false, new NullProgressListener());
//                rv = rv.concat("Group: ").concat(key.toString()).concat(" -> ");
//                if (results != null) {
//                    if (results.getAverage() != null)           rv = rv.concat("Average: ").concat(results.getAverage().toString()).concat(", ");
//                    if (results.getCount() != null)             rv = rv.concat("Count: ").concat(results.getCount().toString()).concat(", ");
//                    if (results.getMax() != null)               rv = rv.concat("Max: ").concat(results.getMax().toString()).concat(", ");
//                    if (results.getMin() != null)               rv = rv.concat("Min: ").concat(results.getMin().toString()).concat(", ");
//                    if (results.getMedian() != null)            rv = rv.concat("Median: ").concat(results.getMedian().toString()).concat(", ");
//                    if (results.getStandardDeviation() != null) rv = rv.concat("StdDev: ").concat(results.getStandardDeviation().toString()).concat(", ");
//                    if (results.getSum() != null)               rv = rv.concat("Sum: ").concat(results.getSum().toString());
//                }
//            }
//            rv = rv.concat("\n");
//        }

//        return new Results();
//        if (rv.length() == 0) {
//            rv = "All ok";
//        }
//        return rv;
        return DataUtilities.simple(new GroupFeatureCollection(features, groupingAttributes, aggregationSpecification));
    }

//    private static HashMap<String, List<AggregationFunction>> getAggregationFunctionsMap(String aggregationSpecification)
//    {
//        HashMap<String, List<AggregationFunction>> rv = new HashMap<String, List<AggregationFunction>>();
//        ArrayList<String> unknownFoos = new ArrayList<String>();
//        if (aggregationSpecification != null) {
//            ArrayList<String> aggrSpec = new ArrayList<String>(Arrays.asList(aggregationSpecification.split(", ")));
//            for (String aggregation : aggrSpec) {
//                String[] aggSplit = aggregation.replace(")", "").split("\\(", 2);
//
//                // We only check aggregation functions
//                boolean ok = false;
//                for (AggregationFunction foo : AggregationFunction.values()) {
//                    if (aggSplit[0].equalsIgnoreCase(foo.name())) {
//                        ok = true;
//                        List<AggregationFunction> foos;
//                        if (rv.containsKey(aggSplit[1])) {
//                            foos = rv.get(aggSplit[1]);
//                        } else {
//                            foos = new ArrayList<AggregationFunction>();
//                        }
//                        foos.add(foo);
//                        rv.put(aggSplit[1], foos);
//                        break;
//                    }
//                }
//
//                if (!ok) {
//                    unknownFoos.add(aggSplit[0]);
//                }
//            }
//        }
//
//        // Throw exception if we found unknown aggregation functions
//        if (!unknownFoos.isEmpty()) {
//            ArrayList<String> valid = new ArrayList<String>();
//            for (AggregationFunction agg : AggregationFunction.values()) {
//                valid.add(agg.name());
//            }
//            String msg = makeExceptionString("Invalid aggregation function(s) ", unknownFoos, ". Valid functions are ", valid);
//            throw new ProcessException(msg);
//        }
//    }

    private static String makeExceptionString(String exception, ArrayList<String> invalidList, String hint, ArrayList<String> validList)
    {
        // Start with the
        String rv = exception;

        // Concat with all invalid values
        ListIterator<String> invalid = invalidList.listIterator();
        while (invalid.hasNext()) {
            if (invalid.hasPrevious()) {
                rv = rv.concat(", ");
            }
            rv = rv.concat(invalid.next());
        }

        // Add a hint with valid values
        rv = rv.concat(hint);
        ListIterator<String> valid = validList.listIterator();
        while (valid.hasNext()) {
            if (valid.hasPrevious()) {
                rv = rv.concat(", ");
            }
            rv = rv.concat(valid.next());
        }

        return rv;
    }

//    private static HashMap<List<Object>, DefaultFeatureCollection> splitFeatures(SimpleFeatureCollection allFeatures, List<String> groupingAttributeNames)
//    {
//        HashMap<List<Object>, DefaultFeatureCollection> featureMap = new HashMap<List<Object>, DefaultFeatureCollection>();
//
//        SimpleFeatureIterator featureIterator = allFeatures.features();
//        while (featureIterator.hasNext()) {
//            SimpleFeature feature = featureIterator.next();
//            ArrayList<Object> groupingValues = new ArrayList<Object>();
//            for (String attr : groupingAttributeNames) {
//                groupingValues.add(feature.getAttribute(attr));
//            }
//
//            if (featureMap.containsKey(groupingValues)) {
//                featureMap.get(groupingValues).add(feature);
//            } else {
//                DefaultFeatureCollection newCollection = new DefaultFeatureCollection();
//                newCollection.add(feature);
//                featureMap.put(groupingValues, newCollection);
//            }
//        }
//        featureIterator.close();
//
//        return featureMap;
//    }


    public static class GroupFeatureCollection extends SimpleProcessingCollection
    {
        private SimpleFeatureCollection delegate;
        private HashMap<String, List<AggregationFunction>> aggrFunctions;
        private HashMap<String, AttributeDescriptor> groupDescriptors;
        private HashMap<String, AttributeDescriptor> aggrDescriptors;
        private SimpleFeatureType featureType;
        private List<SimpleFeature> featureList;

        public GroupFeatureCollection(SimpleFeatureCollection delegate, String groupingAttributes, String aggregationSpecification) throws ProcessException
        {
            this.delegate = delegate;

            ArrayList<String> groupAttrs = new ArrayList<String>(Arrays.asList(groupingAttributes.split(", ")));

            collectAggregationFunctions(aggregationSpecification);
            collectDescriptors(groupAttrs);

            // Split features into FeatureCollections, based on supplied grouping attribute names
            HashMap<List<Object>, DefaultFeatureCollection> featureMap = splitFeatures(this.delegate, groupAttrs);

            // Aggregate and create features
            featureList = aggregate(featureMap);
        }

        @Override
        public SimpleFeatureIterator features()
        {
            return new GroupFeatureIterator(featureList);
        }

        @Override
        public ReferencedEnvelope getBounds()
        {
            return null;
        }

        @Override
        protected SimpleFeatureType buildTargetFeatureType()
        {
            if (featureType == null) {
                SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
                for (String key : groupDescriptors.keySet()) {
                    tb.add(groupDescriptors.get(key));
                }
                for (String key : aggrDescriptors.keySet()) {
                    // TODO: preimenuj AttributeDescriptor, da bo v imenu tudi ime agregatne funkcije
                    tb.add(aggrDescriptors.get(key));
                }
                tb.setName(delegate.getSchema().getName());
                featureType = tb.buildFeatureType();
            }
            return featureType;
        }

        @Override
        public int size()
        {
            return featureList.size();
        }

        private void collectAggregationFunctions(String aggregationSpecification) throws ProcessException
        {
            aggrFunctions = new HashMap<String, List<AggregationFunction>>();
            ArrayList<String> unknownFoos = new ArrayList<String>();
            if (aggregationSpecification != null) {
                ArrayList<String> aggrSpec = new ArrayList<String>(Arrays.asList(aggregationSpecification.split(", ")));
                for (String aggregation : aggrSpec) {
                    String[] aggSplit = aggregation.replace(")", "").split("\\(", 2);

                    // We only check aggregation functions
                    boolean ok = false;
                    for (AggregationFunction foo : AggregationFunction.values()) {
                        if (aggSplit[0].equalsIgnoreCase(foo.name())) {
                            ok = true;
                            List<AggregationFunction> foos;
                            if (aggrFunctions.containsKey(aggSplit[1])) {
                                foos = aggrFunctions.get(aggSplit[1]);
                            } else {
                                foos = new ArrayList<AggregationFunction>();
                            }
                            foos.add(foo);
                            aggrFunctions.put(aggSplit[1], foos);
                            break;
                        }
                    }

                    if (!ok) {
                        unknownFoos.add(aggSplit[0]);
                    }
                }
            }

            // Throw exception if we found unknown aggregation functions
            if (!unknownFoos.isEmpty()) {
                ArrayList<String> valid = new ArrayList<String>();
                for (AggregationFunction agg : AggregationFunction.values()) {
                    valid.add(agg.name());
                }
                String msg = makeExceptionString("Invalid aggregation function(s) ", unknownFoos, ". Valid functions are ", valid);
                throw new ProcessException(msg);
            }
        }

        private void collectDescriptors(ArrayList<String> groupAttrs) throws ProcessException
        {
            List<AttributeDescriptor> allAttrs = this.delegate.getSchema().getAttributeDescriptors();

            // Collect grouping and aggregation attribute indexes and check for their validity
            groupDescriptors = new HashMap<String, AttributeDescriptor>();
            aggrDescriptors = new HashMap<String, AttributeDescriptor>();
            ArrayList<String> unknownAttrs = new ArrayList<String>(groupAttrs);
            ArrayList<String> unknownAggrs = new ArrayList<String>(aggrFunctions.keySet());
            for (int i = 0; i < allAttrs.size(); i++) {
                String attName = allAttrs.get(i).getLocalName();

                // Collect grouping attribute indexes
                for (String groupAttName : unknownAttrs) {
                    if (attName.equals(groupAttName)) {
                        groupDescriptors.put(attName, allAttrs.get(i));
                        unknownAttrs.remove(groupAttName);
                        break;
                    }
                }

                // Check aggregation attributes for validity
                for (String aggAttName : unknownAggrs) {
                    if (attName.equals(aggAttName)) {
                        aggrDescriptors.put(attName, allAttrs.get(i));
                        unknownAggrs.remove(aggAttName);
                        break;
                    }
                }

                if (unknownAttrs.isEmpty() && unknownAggrs.isEmpty()) {
                    break;
                }
            }

            // Throw exception if we found unknown grouping attributes
            if (!unknownAttrs.isEmpty()) {
                ArrayList<String> validAttrNames = new ArrayList<String>();
                for (AttributeDescriptor desc : allAttrs) {
                    validAttrNames.add(desc.getLocalName());
                }
                String msg = makeExceptionString("Could not find grouping attribute(s) ", unknownAttrs, ". The valid values are ", validAttrNames);

                throw new ProcessException(msg);
            }

            // Throw exception if we found unknown aggregation attributes
            if (!unknownAggrs.isEmpty()) {
                ArrayList<String> validAttrNames = new ArrayList<String>();
                for (AttributeDescriptor desc : allAttrs) {
                    validAttrNames.add(desc.getLocalName());
                }
                String msg = makeExceptionString("Could not find aggregation attribute(s) ", unknownAggrs, ". The valid values are ", validAttrNames);

                throw new ProcessException(msg);
            }
        }

        private HashMap<List<Object>, DefaultFeatureCollection> splitFeatures(SimpleFeatureCollection allFeatures, List<String> groupingAttributeNames)
        {
            HashMap<List<Object>, DefaultFeatureCollection> featureMap = new HashMap<List<Object>, DefaultFeatureCollection>();

            SimpleFeatureIterator featureIterator = allFeatures.features();
            while (featureIterator.hasNext()) {
                SimpleFeature feature = featureIterator.next();
                ArrayList<Object> groupingValues = new ArrayList<Object>();
                for (String attr : groupingAttributeNames) {
                    groupingValues.add(feature.getAttribute(attr));
                }

                if (featureMap.containsKey(groupingValues)) {
                    featureMap.get(groupingValues).add(feature);
                } else {
                    DefaultFeatureCollection newCollection = new DefaultFeatureCollection();
                    newCollection.add(feature);
                    featureMap.put(groupingValues, newCollection);
                }
            }
            featureIterator.close();

            return featureMap;
        }

        private List<SimpleFeature> aggregate(HashMap<List<Object>, DefaultFeatureCollection> featureMap)
        {
            // Aggregate on each split FeatureCollection
            HashMap<List<Object>, List<Results>> resultsMap = new HashMap<List<Object>, List<Results>>();
            AggregateProcess aggregateProcess = new AggregateProcess();
            for (List<Object> key : featureMap.keySet()) {
                ArrayList<Results> resultsList = new ArrayList<Results>();
                DefaultFeatureCollection fc = featureMap.get(key);
                for (String agg : aggrFunctions.keySet()) {
                    try {
                        Results results = aggregateProcess.execute(fc, agg, new HashSet<AggregationFunction>(aggrFunctions.get(agg)), false, new NullProgressListener());
                        resultsList.add(results);
                    } catch (IOException e) {
                        throw new ProcessException(e.getCause());
                    }
                }
                resultsMap.put(key, resultsList);
            }

            // Create features
            List<SimpleFeature> rv = new ArrayList<SimpleFeature>();
            SimpleFeatureBuilder fb = new SimpleFeatureBuilder(buildTargetFeatureType());
            Integer iterationIndex = 1;
            for (List<Object> key : resultsMap.keySet()) {
                // Add grouping attributes
                fb.addAll(key);

                // Add aggregating attributes
                List<Results> results = resultsMap.get(key);
                for (Results r : results) {
                    if (r.getAverage() != null)             fb.add(r.getAverage());
                    if (r.getCount() != null)               fb.add(r.getCount());
                    if (r.getMax() != null)                 fb.add(r.getMax());
                    if (r.getMin() != null)                 fb.add(r.getMin());
                    if (r.getMedian() != null)              fb.add(r.getMedian());
                    if (r.getStandardDeviation() != null)   fb.add(r.getStandardDeviation());
                    if (r.getSum() != null)                 fb.add(r.getSum());
                }

                // Create feature
                rv.add(fb.buildFeature(iterationIndex.toString()));
                iterationIndex++;
            }

            return rv;
        }
    }

    public static class GroupFeatureIterator implements SimpleFeatureIterator
    {
        private ListIterator<SimpleFeature> iterator;

        public GroupFeatureIterator(List<SimpleFeature> featureList)
        {
            this.iterator = featureList.listIterator();
        }

        @Override
        public boolean hasNext()
        {
            return this.iterator.hasNext();
        }

        @Override
        public SimpleFeature next() throws NoSuchElementException
        {
            return this.iterator.next();
        }

        @Override
        public void close()
        {
            this.iterator = null;
        }
    }

}
