package com.croeder.learn;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.binary.Hex;

public class DoSha256 {

    public final static String test_message =  "a|123445|chris|roeder|123 S. Main St|Ft Morgan|CO|80000";

    public static void do_it() {
        System.out.println("input " + test_message + ", output:" +
            convertToHexDigest(test_message));
    }

    public static String convertToHexDigest(String input) {
        byte[] digest_bytes = DigestUtils.sha256(input);
        return(Hex.encodeHexString(digest_bytes));
    }

}
