package com.cis555.search.crawler.info;

import com.cis555.search.enums.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RobotsTxt {
    private static Logger logger = LoggerFactory.getLogger(RobotsTxt.class);
    private Map<String, List<Object>> blacklist;
    private Map<String, List<Object>> whitelist;
    private Map<String, Integer> delays;
    private List<String> userAgents;

    public static final RobotsTxt dummy = new RobotsTxt();

    private RobotsTxt() {
        blacklist = new HashMap<>();
        whitelist = new HashMap<>();
        delays = new HashMap<>();
        userAgents = new ArrayList<>();
    }

    public RobotsTxt(String robotsTxt, String userAgentInterested) {
        this();
        String currentUserAgent = null;
        String[] lines = robotsTxt.split(Constants.ROBOT_SPLIT.value());
        for (String line : lines) {
            // 1. Remove all the comments within the robots.txt
            int pos = line.indexOf("#");
            if (pos != -1) {
                line = line.substring(0, pos);
            }

            // 2. If the line is empty, skip
            line = line.trim().toLowerCase();
            if (line.length() == 0 || line.charAt(0) == '\uFEFF') {
                continue;
            }

            // 3. Parse each line
            if (line.startsWith("user-agent")) {
                line = line.substring(10).trim();
                line = line.startsWith(":") ? line.substring(1) : line;
                if (userAgentInterested.startsWith(line) || line.equals("*")) {
                    userAgents.add(line);
                    currentUserAgent = line;
                } else {
                    currentUserAgent = null;
                }
            } else if (currentUserAgent != null && line.startsWith("disallow")) {
                line = line.substring(8).trim();
                line = line.startsWith(":") ? line.substring(1) : line;
                addToDisallowed(currentUserAgent, line);
            } else if (currentUserAgent != null && line.startsWith("allow")) {
                line = line.substring(5).trim();
                line = line.startsWith(":") ? line.substring(1) : line;
                addToAllowed(currentUserAgent, line);
            } else if (currentUserAgent != null && line.startsWith("crawl-delay")) {
                line = line.substring(11).trim();
                line = line.startsWith(":") ? line.substring(1) : line;
                try {
                    delays.put(currentUserAgent, Integer.parseInt(line));
                } catch (NumberFormatException e) {
                    logger.error("NumberFormatException when parsing the robots.txt");
                }
            }
        }
    }

    public void addToDisallowed(String key, String link) {
        List<Object> list = blacklist.getOrDefault(key, new ArrayList<>());
        Object value;
        if (link.contains("*") || link.endsWith("$")) {
            value = Pattern.compile(wildcardToRegex(link));
        } else {
            value = link;
        }
        list.add(value);
        blacklist.putIfAbsent(key, list);
    }

    public void addToAllowed(String key, String link) {
        List<Object> list = whitelist.getOrDefault(key, new ArrayList<>());
        Object value;
        if (link.contains("*") || link.endsWith("$")) {
            value = Pattern.compile(wildcardToRegex(link));
        } else {
            value = link;
        }
        list.add(value);
        whitelist.putIfAbsent(key, list);
    }

    public int getDelay(String key, int time) {
        Integer delay = delays.get(key);
        if (delay == null) {
            delay = delays.get("*");
            if (delay == null) {
                return time;
            }
        }
        return delay;
    }

    public boolean isURLAllowed(URLInfo url, String userAgent) {

        // 1. Return true if userAgent is empty
        if (this.userAgents.isEmpty()) {
            return true;
        }

        // 2. Pick a matched Agent
        String matchedAgent = null;
        for (String temp : this.userAgents) {
            if (userAgent.startsWith(temp)) {
                matchedAgent = temp;
                break;
            }
        }
        if (matchedAgent == null) {
            if (userAgents.contains("*")) {
                matchedAgent = "*";
            } else {
                return true;
            }
        }

        String path = url.getFilePath();
        // 3. Matching with whitelist
        List<Object> allowed = whitelist.get(matchedAgent);
        if (allowed != null) {
            for (Object condition : allowed) {
                if (condition instanceof String) {
                    String prefix = (String) condition;
                    if (path.startsWith(prefix)) {
                        return true;
                    }
                } else {
                    Pattern pattern = (Pattern) condition;
                    Matcher matcher = pattern.matcher(path);
                    if (matcher.find()) {
                        return true;
                    }
                }
            }
        }

        // 4. Matching with blacklist
        List<Object> disallowedLinks = blacklist.get(matchedAgent);
        if (disallowedLinks != null) {
            for (Object condition : disallowedLinks) {
                if (condition instanceof String) {
                    String prefix = (String) condition;
                    if (prefix.length() == 0) {
                        return true;
                    }
                    if (path.startsWith(prefix)) {
                        return false;
                    }
                } else {
                    Pattern pattern = (Pattern) condition;
                    Matcher matcher = pattern.matcher(path);
                    if (matcher.find()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public boolean isUninitialized() {
        return whitelist.isEmpty() && blacklist.isEmpty() && delays.isEmpty() && delays.isEmpty() && userAgents.isEmpty();
    }

    private static final String wildcardToRegex(String wc) {
        String pattern = regexEscape(wc).replace("*", ".*");
        return "^" + pattern;
    }

    public static final String regexEscape(String aRegexFragment) {
        StringBuilder sb = new StringBuilder();
        StringCharacterIterator itor = new StringCharacterIterator(aRegexFragment);
        char ch = itor.current();
        Set<Character> set = getCharSet();
        while (ch != CharacterIterator.DONE) {
            if (set.contains(ch)) {
                sb.append("\\");
            }
            sb.append(ch);
            ch = itor.next();
        }
        return sb.toString();
    }

    private static Set<Character> getCharSet() {
        String template = ".\\/?+&:{}[]()^";
        Set<Character> set = new HashSet<>();
        for (Character c : template.toCharArray()) {
            set.add(c);
        }
        return set;
    }
}
