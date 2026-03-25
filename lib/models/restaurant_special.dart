class RestaurantSpecial {
  final String id;
  final String title;
  final String description;
  final String startDate;
  final String endDate;
  final String restaurantName;
  final String? restaurantAddress;
  final String? restaurantPhone;
  final String? restaurantWebsite;
  final String? imageUrl;
  final int priority;
  final String createdAt;

  const RestaurantSpecial({
    required this.id,
    required this.title,
    required this.description,
    required this.startDate,
    required this.endDate,
    required this.restaurantName,
    this.restaurantAddress,
    this.restaurantPhone,
    this.restaurantWebsite,
    this.imageUrl,
    required this.priority,
    required this.createdAt,
  });

  factory RestaurantSpecial.fromJson(Map<String, dynamic> json) {
    return RestaurantSpecial(
      id: json['id'] as String? ?? '',
      title: json['title'] as String? ?? '',
      description: json['description'] as String? ?? '',
      startDate: json['start_date'] as String? ?? '',
      endDate: json['end_date'] as String? ?? '',
      restaurantName: json['restaurant_name'] as String? ?? '',
      restaurantAddress: json['restaurant_address'] as String?,
      restaurantPhone: json['restaurant_phone'] as String?,
      restaurantWebsite: json['restaurant_website'] as String?,
      imageUrl: json['image_url'] as String?,
      priority: json['priority'] as int? ?? 0,
      createdAt: json['created_at'] as String? ?? '',
    );
  }

  /// Returns true if the special is currently active.
  /// Shows one day early so Tuesday afternoon posts appear before Wednesday.
  bool get isActive {
    final start = DateTime.tryParse(startDate);
    final end = DateTime.tryParse(endDate);
    if (start == null || end == null) return false;

    final now = DateTime.now();
    final nowDay = DateTime(now.year, now.month, now.day);
    final startDay = DateTime(start.year, start.month, start.day);
    final endDay = DateTime(end.year, end.month, end.day);
    // Preview one day early
    final previewDay = startDay.subtract(const Duration(days: 1));

    return nowDay.compareTo(previewDay) >= 0 && nowDay.compareTo(endDay) <= 0;
  }
}

class RestaurantSpecialsResponse {
  final List<RestaurantSpecial> specials;
  final String lastUpdated;

  const RestaurantSpecialsResponse({
    required this.specials,
    required this.lastUpdated,
  });

  factory RestaurantSpecialsResponse.fromJson(Map<String, dynamic> json) {
    final rawSpecials = json['specials'] as List<dynamic>? ?? [];
    return RestaurantSpecialsResponse(
      specials: rawSpecials
          .whereType<Map<String, dynamic>>()
          .map(RestaurantSpecial.fromJson)
          .toList(),
      lastUpdated: json['last_updated'] as String? ?? '',
    );
  }
}
