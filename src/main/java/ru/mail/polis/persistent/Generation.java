package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

final class Generation {
    private Generation(){}

    static long fromPath(@NotNull final Path path) {
        return getNumericValue(path.getFileName().toString());
    }

    /**
     * Get generation by name of table.
     * @param name is the name of file
     **/
    private static long getNumericValue(@NotNull final String name){
        final StringBuilder res = new StringBuilder();
        for(int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            if(Character.isDigit(c)) {
                res.append(c);
            }
        }
        return Long.parseLong(res.toString());
    }
}
