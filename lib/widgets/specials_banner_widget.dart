import 'package:flutter/material.dart';
import '../models/restaurant_special.dart';
import '../screens/specials_screen.dart';

/// Compact amber banner that shows the first active special with a "+N more"
/// badge and navigates to SpecialsScreen on tap.
class SpecialsBannerWidget extends StatefulWidget {
  final List<RestaurantSpecial> activeSpecials;

  const SpecialsBannerWidget({super.key, required this.activeSpecials});

  @override
  State<SpecialsBannerWidget> createState() => _SpecialsBannerWidgetState();
}

class _SpecialsBannerWidgetState extends State<SpecialsBannerWidget> {
  bool _pressed = false;

  @override
  Widget build(BuildContext context) {
    if (widget.activeSpecials.isEmpty) return const SizedBox.shrink();

    final first = widget.activeSpecials.first;
    final extras = widget.activeSpecials.length - 1;

    return GestureDetector(
      onTapDown: (_) => setState(() => _pressed = true),
      onTapUp: (_) {
        setState(() => _pressed = false);
        Navigator.push(
          context,
          MaterialPageRoute(builder: (_) => const SpecialsScreen()),
        );
      },
      onTapCancel: () => setState(() => _pressed = false),
      child: AnimatedScale(
        scale: _pressed ? 0.97 : 1.0,
        duration: const Duration(milliseconds: 100),
        child: Container(
          margin: const EdgeInsets.fromLTRB(12, 8, 12, 4),
          decoration: BoxDecoration(
            gradient: const LinearGradient(
              colors: [Color(0xFFFFB300), Color(0xFFFF6F00)],
              begin: Alignment.centerLeft,
              end: Alignment.centerRight,
            ),
            borderRadius: BorderRadius.circular(14),
            boxShadow: [
              BoxShadow(
                color: const Color(0xFFFFB300).withOpacity(0.35),
                blurRadius: 8,
                offset: const Offset(0, 3),
              ),
            ],
          ),
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
            child: Row(
              children: [
                // Logo or icon
                ClipRRect(
                  borderRadius: BorderRadius.circular(8),
                  child: first.imageUrl != null && first.imageUrl!.isNotEmpty
                      ? Image.network(
                          first.imageUrl!,
                          width: 40,
                          height: 40,
                          fit: BoxFit.cover,
                          errorBuilder: (_, __, ___) => _fallbackIcon(),
                        )
                      : _fallbackIcon(),
                ),

                const SizedBox(width: 10),

                // Text
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Row(
                        children: [
                          const Icon(
                            Icons.local_offer,
                            size: 11,
                            color: Colors.white,
                          ),
                          const SizedBox(width: 4),
                          const Text(
                            'WEEKLY SPECIALS',
                            style: TextStyle(
                              fontSize: 10,
                              fontWeight: FontWeight.w700,
                              color: Colors.white,
                              letterSpacing: 0.8,
                            ),
                          ),
                        ],
                      ),
                      const SizedBox(height: 2),
                      Text(
                        first.restaurantName,
                        style: const TextStyle(
                          fontSize: 14,
                          fontWeight: FontWeight.w800,
                          color: Colors.white,
                        ),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                      if (first.restaurantAddress != null &&
                          first.restaurantAddress!.isNotEmpty)
                        Text(
                          first.restaurantAddress!,
                          style: TextStyle(
                            fontSize: 11,
                            color: Colors.white.withOpacity(0.85),
                          ),
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                        ),
                    ],
                  ),
                ),

                const SizedBox(width: 8),

                // "+N more" badge or chevron
                if (extras > 0)
                  Container(
                    padding: const EdgeInsets.symmetric(
                        horizontal: 8, vertical: 4),
                    decoration: BoxDecoration(
                      color: Colors.white.withOpacity(0.25),
                      borderRadius: BorderRadius.circular(20),
                    ),
                    child: Text(
                      '+$extras more',
                      style: const TextStyle(
                        fontSize: 11,
                        fontWeight: FontWeight.w700,
                        color: Colors.white,
                      ),
                    ),
                  )
                else
                  const Icon(
                    Icons.chevron_right,
                    color: Colors.white,
                    size: 20,
                  ),
              ],
            ),
          ),
        ),
      ),
    );
  }

  Widget _fallbackIcon() {
    return Container(
      width: 40,
      height: 40,
      decoration: BoxDecoration(
        color: Colors.white.withOpacity(0.25),
        borderRadius: BorderRadius.circular(8),
      ),
      child: const Icon(Icons.restaurant, color: Colors.white, size: 20),
    );
  }
}
