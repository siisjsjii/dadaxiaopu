package com.cinfly.dadaxiaopu.utils;

import com.cinfly.dadaxiaopu.dto.UserDTO;
import com.cinfly.dadaxiaopu.entity.User;

public class UserHolder {
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    public static void saveUser(UserDTO user){
        tl.set(user);
    }

    public static UserDTO getUser(){
        return tl.get();
    }

    public static void removeUser(){
        tl.remove();
    }
}
