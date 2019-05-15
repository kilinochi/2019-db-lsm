package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Generation {
    private Generation(){}



    public static long fromPath(@NotNull final Path path) {
        return getNumericValue(path.getFileName().toString());
    }

    /**
     * Get generation by name of table.
     * @param name is the name of file
     **/


    public static long getNumericValue(@NotNull final String name){
        StringBuilder res = new StringBuilder();
        final String [] tmp0 = name.split("/");
        final String tmp1 = tmp0[tmp0.length - 1];
        for(int i = 0; i < tmp1.length(); i++) {
            final char c = tmp1.charAt(i);
            if( c > 47 && c < 58) {
                res.append(c);
            }
        }
        return Long.parseLong(res.toString());
    }
}
