package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

public final class Generation {
    private Generation(){}

    /**
     * Get generation by name of table.
     * @param name is the name of file
     **/


    public static long getNumericValue(@NotNull final String name){
        final StringBuilder res = new StringBuilder();
        final String [] tmp0 = name.split("/");
        final String tmp1 = tmp0[tmp0.length - 1];
        for (int i = 0; i < tmp1.length(); i++) {
            final char tmp = tmp1.charAt(i);
            if(Character.isDigit(tmp)) {
                res.append(tmp);
            }
        }
        return Long.parseLong(res.toString());
    }
}
