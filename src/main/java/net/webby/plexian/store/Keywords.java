package net.webby.plexian.store;

import java.util.ArrayList;
import java.util.List;

public class Keywords {

    List<String> words = new ArrayList<String>();
    
    public Keywords(String keywords) {
        if (keywords != null) {
            for (String word : keywords.split(" ")) {
                if (word.length() > 0) {
                    words.add(word);
                }
            }
        }
    }
    
    public int size() {
        return words.size();
    }
    
    public String join(int offset, int size) {
        if (size == 1) {
            return words.get(offset);
        }
        StringBuilder str = new StringBuilder();
        for (int i = offset; i < offset + size && i < words.size(); ++i) {
            if (str.length() > 0) {
                str.append(' ');
            }
            str.append(words.get(i));
        }
        return str.toString();
    }
}
