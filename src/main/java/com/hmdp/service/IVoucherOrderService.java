package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

public interface IVoucherOrderService extends IService<VoucherOrder> {
    // 加上事务(一般在接口类写)
    // @Transactional  这里就没有事务了 全在我们的另外一个方法里面了

    Result seckillVoucher(Long voucherId);


    void createVoucherOrder(VoucherOrder voucherId);
}
