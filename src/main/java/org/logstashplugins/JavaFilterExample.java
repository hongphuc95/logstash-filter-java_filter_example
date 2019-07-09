package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// class name must match plugin name
@LogstashPlugin(name = "java_filter_example")
public class JavaFilterExample implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "message");

    private String id;
    private String sourceField;

    public HashMap<String, String> patternRegexMatching(String message) {
        HashMap<String, String> infoCollected =  new HashMap<>();
        HashMap<String, String> patternHash = new HashMap<>();
        patternHash.put("EndJob", "\\*.*Fin du job.*\\*");
        patternHash.put("EndAnormal", "\\*.*Fin anormale.*\\*");
        patternHash.put("Step", "\\*\\-*\\s*STEP\\w+\\s*\\-*\\*");
        patternHash.put("JobDate", "JOB.*DATE.*\\d{4}\\W\\d{2}\\W\\d{2}\\s*\\d{2}\\W\\d{2}\\W\\d{2}");
        patternHash.put("Info", "\\w{8}\\-\\w{8}\\-.*");

        String matchingStatus = null;
        for (String key : patternHash.keySet())
        {
            String regex = patternHash.get(key);
            Pattern p = Pattern.compile(regex);
            Matcher m = p.matcher(message);
            boolean isMatched = m.matches();
            if (isMatched == true) {
                matchingStatus = key;
                infoCollected = extractInformation(matchingStatus, message);
                break;
            }
        }
        return infoCollected;

    }

    public HashMap<String, String> extractInformation(String statusMatched, String message) {
        HashMap<String, String> information = new HashMap<>();
        Pattern p;
        Matcher m;
        if (statusMatched == "Step") {
            p = Pattern.compile("\\*\\-*\\s*STEP(\\w+)\\s*\\-*\\*");
            m = p.matcher(message);
            if (m.find()) {
                if (!m.group(1).equals("0000")) {
                    information.put("step", m.group(1));
                }
            }
        } else if (statusMatched == "JobDate") {
            p = Pattern.compile("JOB\\W+(\\w{4})\\s+DATE\\W+(\\d{4}\\W\\d{2}\\W\\d{2}\\s*\\d{2}\\W\\d{2}\\W\\d{2})");
            m = p.matcher(message);
            if (m.find()) {
                if (m.group(1) != "EXECBOB") {
                    information.put("job", m.group(1));
                    information.put("date", m.group(2));
                }
            }

        } else if (statusMatched == "Info") {
            p = Pattern.compile("(\\w{8})\\-\\w{8}\\-(.*)");
            m = p.matcher(message);
            String extraInfo = "";
            if (m.find()) {
                if (!m.group(1).contains("BOB")) {
                    information.put("idProcess", m.group(1));
                    extraInfo = m.group(2);
                }
            }

            if (extraInfo.contains("HORODATAGE DE DEBUT")) {
                Matcher mstartProcess = Pattern.compile("(\\d{4}\\-\\d{2}\\-\\d{2}\\-\\d{2}\\.\\d{2}\\.\\d{2})").matcher(extraInfo);
                if (mstartProcess.find()) {
                    information.put("startProcess", mstartProcess.group(1));
                }
            } else if (extraInfo.contains("HORODATAGE DE FIN")) {
                Matcher mendProcess = Pattern.compile("(\\d{4}\\-\\d{2}\\-\\d{2}\\-\\d{2}\\.\\d{2}\\.\\d{2})").matcher(extraInfo);
                if (mendProcess.find()) {
                    information.put("endProcess", mendProcess.group(1));
                }
            }
        }

        return information;
    }

    public JavaFilterExample(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        Collection<Event> eventFiltered = new ArrayList<>();
        HashMap<String, HashMap> stepBlock = new HashMap<>();
        String job = null;
        String startJob = null;
        String endJob = null;
        String step = null;
        int counterTimestamp = 0;
        for (Event e : events) {
            Object f = e.getField(sourceField);
            if (f instanceof String) {
                HashMap<String, String> infoCollected = new HashMap<>();
                infoCollected = patternRegexMatching((String) f);
                if (!infoCollected.isEmpty()) {
                    for (String infoKey : infoCollected.keySet()) {
                        if (infoKey == "job") {
                            job = infoCollected.get(infoKey);
                        } else if (infoKey == "step") {
                            step = infoCollected.get(infoKey);
                            stepBlock.put(step, new HashMap<String, String>());
                        } else if (infoKey == "date") {
                            if (counterTimestamp == 0) {
                                startJob = infoCollected.get(infoKey);
                                counterTimestamp++;
                            } else if (counterTimestamp == 1) {
                                endJob = infoCollected.get(infoKey);
                            }
                        } else if (infoKey == "startProcess" || infoKey == "endProcess") {
                            if (step != null) {
                                stepBlock.get(step).put(infoKey, infoCollected.get(infoKey));
                            }
                        }


                    }
                }
            }
        }

        int count = 0;
        for (String keyEventCollected : stepBlock.keySet()) {
            Event event;

            HashMap<String, String> stepBlockElement = stepBlock.get(keyEventCollected);
            if (!stepBlockElement.isEmpty()) {
                event = new org.logstash.Event();
                System.out.println(count + " --- " + keyEventCollected);
                count++;
                event.setField("step", keyEventCollected);
                for (String keyCollected : stepBlockElement.keySet()) {
                    event.setField(keyCollected, stepBlockElement.get(keyCollected));
                }
                eventFiltered.add(event);
            }
        }
        //events.removeAll(nonMatchedList);
        return eventFiltered;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Collections.singletonList(SOURCE_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
