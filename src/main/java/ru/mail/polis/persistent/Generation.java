package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Generation {
    private Generation(){}



    public static long fromPath(@NotNull final Path path) {
        return getNumericValue(path.getFileName().toString());
    }


    public static long fromFile(@NotNull final File file) {
        return getNumericValue(file.getName());
    }


    /**
     * Get generation by name of table.
     * @param name is the name of file
     **/


    public static long getNumericValue(@NotNull final String name){
        final Pattern rgx = Pattern.compile(CustomDAO.FILE_NAME + "(\\d)" + CustomDAO.SUFFIX_DAT);
        final Matcher matcher = rgx.matcher(name);
        if(matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1L;
    }
}
