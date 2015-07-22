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

    // TODO: Extend aggregation functions and include geometrical functions

    @DescribeResult(name = "result", description = "Grouping results")
    public SimpleFeatureCollection execute(
            @DescribeParameter(name = "features", description = "Input feature collection") SimpleFeatureCollection features,
            @DescribeParameter(name = "groupingAttributes", min = 1, description = "A list of attributes on which to perform grouping, delimited by commas.") String groupingAttributes,
            @DescribeParameter(name = "aggregationSpecification", min = 0, description = "The aggregation specification of the form aggregation(attribute), delimited by commas.") String aggregationSpecification
    ) throws ProcessException, IOException
    {
        LinkedList<String> groupAttrs = null;
        List<AttributeDescriptor> allAttrs;

        // Check if mandatory parameters are not null
        if (features == null) {
            throw new ProcessException("Please supply a feature collection");
        }
        if (groupingAttributes == null) {
            throw new ProcessException("Please specify the grouping attribute(s)");
        } else {
            groupAttrs = new LinkedList<String>(Arrays.asList(groupingAttributes.split(", ")));
        }

        // Check if the feature collection has more attributes than the grouping attributes list
        allAttrs = features.getSchema().getAttributeDescriptors();
        if (groupAttrs.size() > allAttrs.size()) {
            throw new ProcessException("The count of supplied grouping attributes is larger than the count of all attributes in the feature collection");
        }

        return DataUtilities.simple(new GroupFeatureCollection(features, groupingAttributes, aggregationSpecification));
    }

    public static class GroupFeatureCollection extends SimpleProcessingCollection
    {
        private SimpleFeatureCollection delegate;
        private LinkedList<String> groupAttrs;
        private HashMap<String, List<AggregationFunction>> aggrFunctions;   // key = aggregation attr name, value = list of aggregation functions
        private HashMap<String, AttributeDescriptor> groupDescriptors;      // key = name of the grouping attribute, value = attribute descriptor
//        private HashMap<String, AttributeDescriptor> aggrDescriptors;       // key = name of the aggregation attribute, value = attribute descriptor
        private SimpleFeatureType featureType;
        private List<SimpleFeature> featureList;

        public GroupFeatureCollection(SimpleFeatureCollection delegate, String groupingAttributes, String aggregationSpecification) throws ProcessException
        {
            this.delegate = delegate;

            groupAttrs = new LinkedList<String>(Arrays.asList(groupingAttributes.split(", ")));

            collectAggregationFunctions(aggregationSpecification);
            collectDescriptors(groupAttrs);

            // Split features into FeatureCollections, based on supplied grouping attribute names
            HashMap<LinkedList<Object>, DefaultFeatureCollection> featureMap = splitFeatures(this.delegate, groupAttrs);

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
                for (String key : groupAttrs) {
                    tb.add(groupDescriptors.get(key));
                }

                for (String key : aggrFunctions.keySet()) {
                    List<AggregationFunction> functions = aggrFunctions.get(key);
                    for (AggregationFunction f : functions) {
                        String name = key.concat("-").concat(f.name());
                        AttributeTypeBuilder atb = new AttributeTypeBuilder();
                        atb.setName(name);
                        atb.setBinding(Number.class);
                        tb.add(atb.buildDescriptor(name));
                    }
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

        private String makeExceptionString(String exception, LinkedList<String> invalidList, String hint, LinkedList<String> validList)
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

        private void collectAggregationFunctions(String aggregationSpecification) throws ProcessException
        {
            aggrFunctions = new HashMap<String, List<AggregationFunction>>();
            LinkedList<String> unknownFoos = new LinkedList<String>();
            if (aggregationSpecification != null) {
                LinkedList<String> aggrSpec = new LinkedList<String>(Arrays.asList(aggregationSpecification.split(", ")));
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
                                foos = new LinkedList<AggregationFunction>();
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
                LinkedList<String> valid = new LinkedList<String>();
                for (AggregationFunction agg : AggregationFunction.values()) {
                    valid.add(agg.name());
                }
                String msg = makeExceptionString("Invalid aggregation function(s) ", unknownFoos, ". Valid functions are ", valid);
                throw new ProcessException(msg);
            }
        }

        private void collectDescriptors(LinkedList<String> groupAttrs) throws ProcessException
        {
            List<AttributeDescriptor> allAttrs = this.delegate.getSchema().getAttributeDescriptors();

            // Collect grouping and aggregation attribute indexes and check for their validity
            groupDescriptors = new HashMap<String, AttributeDescriptor>();
            HashMap<String, AttributeDescriptor> aggrDescriptors = new HashMap<String, AttributeDescriptor>();
            LinkedList<String> unknownAttrs = new LinkedList<String>(groupAttrs);
            LinkedList<String> unknownAggrs = new LinkedList<String>(aggrFunctions.keySet());
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
                LinkedList<String> validAttrNames = new LinkedList<String>();
                for (AttributeDescriptor desc : allAttrs) {
                    validAttrNames.add(desc.getLocalName());
                }
                String msg = makeExceptionString("Could not find grouping attribute(s) ", unknownAttrs, ". The valid values are ", validAttrNames);

                throw new ProcessException(msg);
            }

            // Throw exception if we found unknown aggregation attributes
            if (!unknownAggrs.isEmpty()) {
                LinkedList<String> validAttrNames = new LinkedList<String>();
                for (AttributeDescriptor desc : allAttrs) {
                    validAttrNames.add(desc.getLocalName());
                }
                String msg = makeExceptionString("Could not find aggregation attribute(s) ", unknownAggrs, ". The valid values are ", validAttrNames);

                throw new ProcessException(msg);
            }
        }

        private HashMap<LinkedList<Object>, DefaultFeatureCollection> splitFeatures(SimpleFeatureCollection allFeatures, List<String> groupingAttributeNames)
        {
            HashMap<LinkedList<Object>, DefaultFeatureCollection> featureMap = new HashMap<LinkedList<Object>, DefaultFeatureCollection>();

            SimpleFeatureIterator featureIterator = allFeatures.features();
            while (featureIterator.hasNext()) {
                SimpleFeature feature = featureIterator.next();
                LinkedList<Object> groupingValues = new LinkedList<Object>();
                for (String attr : groupingAttributeNames) {
                    Object val = feature.getAttribute(attr);
                    if (val == null) {
                        groupingValues.add("");
                    } else {
                        groupingValues.add(val);
                    }
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

        private List<SimpleFeature> aggregate(HashMap<LinkedList<Object>, DefaultFeatureCollection> featureMap)
        {
            // Aggregate on each split FeatureCollection
            HashMap<LinkedList<Object>, HashMap<String, Results>> resultsMap = new HashMap<LinkedList<Object>, HashMap<String, Results>>();
            AggregateProcess aggregateProcess = new AggregateProcess();
            for (LinkedList<Object> key : featureMap.keySet()) {
                HashMap<String, Results> attResultsMap = new HashMap<String, Results>();
                DefaultFeatureCollection fc = featureMap.get(key);
                for (String att : aggrFunctions.keySet()) {
                    try {
                        Results results = aggregateProcess.execute(fc, att, new HashSet<AggregationFunction>(aggrFunctions.get(att)), false, new NullProgressListener());
                        attResultsMap.put(att, results);
                    } catch (IOException e) {
                        throw new ProcessException(e.getCause());
                    }
                }
                resultsMap.put(key, attResultsMap);
            }

            // Create features
            List<SimpleFeature> rv = new LinkedList<SimpleFeature>();
            SimpleFeatureBuilder fb = new SimpleFeatureBuilder(buildTargetFeatureType());
            Integer iterationIndex = 1;
            for (LinkedList<Object> key : resultsMap.keySet()) {
                // Add grouping attributes
                fb.addAll(key);

                // Add aggregating attributes
                HashMap<String, Results> attResultsMap = resultsMap.get(key);
                for (String att : aggrFunctions.keySet()) {
                    Results r = attResultsMap.get(att);
                    for (AggregationFunction foo : aggrFunctions.get(att)) {
                        if (foo == AggregationFunction.Average && r.getAverage() != null)           fb.add(r.getAverage());
                        if (foo == AggregationFunction.Count && r.getCount() != null)               fb.add(r.getCount());
                        if (foo == AggregationFunction.Max && r.getMax() != null)                   fb.add(r.getMax());
                        if (foo == AggregationFunction.Min && r.getMin() != null)                   fb.add(r.getMin());
                        if (foo == AggregationFunction.Median && r.getMedian() != null)             fb.add(r.getMedian());
                        if (foo == AggregationFunction.StdDev && r.getStandardDeviation() != null)  fb.add(r.getStandardDeviation());
                        if (foo == AggregationFunction.Sum && r.getSum() != null)                   fb.add(r.getSum());
                    }
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
