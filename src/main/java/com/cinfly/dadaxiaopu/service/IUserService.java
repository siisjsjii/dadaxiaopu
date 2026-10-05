package com.cinfly.dadaxiaopu.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.cinfly.dadaxiaopu.dto.LoginFormDTO;
import com.cinfly.dadaxiaopu.dto.Result;
import com.cinfly.dadaxiaopu.entity.User;

import javax.servlet.http.HttpSession;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author cinfly
 * @since 2021-12-22
 */
public interface IUserService extends IService<User> {

    Result sendCode(String phone);

    Result login(LoginFormDTO loginForm);

    Result sign();

    Result signCount();
}
