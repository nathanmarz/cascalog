package jcascalog;

import java.util.ArrayList;

public class Fields extends ArrayList<Object> {
    public Fields(Object... fields) {
        for(Object field: fields) {
            add(field);
        }
    }
}
