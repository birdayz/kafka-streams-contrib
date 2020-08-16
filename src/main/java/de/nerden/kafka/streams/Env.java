package de.nerden.kafka.streams;

public class Env {

    /**
     * Returns environment variable for given key or defaultValue if it doesn't exist or SecurityManager throws exception
     */
    public static String get(String key, String defaultValue) {
        try {
            String ret = System.getenv(key);
            if (ret == null || ret.length() == 0) {
                return defaultValue;
            }
            return ret;
        } catch (Exception e) {
            return defaultValue;
        }
    }
}

